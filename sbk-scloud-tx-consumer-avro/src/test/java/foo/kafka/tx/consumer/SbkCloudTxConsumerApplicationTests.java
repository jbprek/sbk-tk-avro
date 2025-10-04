package foo.kafka.tx.consumer;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatDao;
import foo.kafka.tx.consumer.persistence.BirthStatEntry;
import foo.kafka.tx.consumer.persistence.BirthStatEntryRepository;
import foo.kafka.tx.consumer.service.EventMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.core.env.Environment;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(
        partitions = 1,
        topics = {"birth.register.avro"},
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        }
)
class SbkCloudTxConsumerApplicationTests {

    @TestConfiguration
    static class KafkaTestConfig {
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        @Bean
        public KafkaTemplate<String, BirthEvent> kafkaTemplate(org.springframework.core.env.Environment env) {
            Map<String, Object> props = new HashMap<>();
            props.put("bootstrap.servers", env.getProperty("spring.kafka.bootstrap-servers", bootstrapServers));
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("schema.registry.url", "mock://embedded-registry");
            props.put("specific.avro.reader", true);

            DefaultKafkaProducerFactory<String, BirthEvent> pf = new DefaultKafkaProducerFactory<>(props);
            // In tests we don't need a transactional producer; keep the default non-transactional producer
            return new KafkaTemplate<>(pf);
        }
    }

    @Autowired
    private KafkaAdmin kafkaAdmin;
    @Autowired
    private KafkaTemplate<String, BirthEvent> kafkaTemplate;
    @Autowired
    private EventMapper mapper;
    private String topic = "birth.register.avro";

    @MockitoSpyBean
    BirthStatDao dao;
    @Autowired
    BirthStatEntryRepository repository;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private Environment env;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        await().pollInterval(1, SECONDS)
                .atMost(3, SECONDS)
                .until(() -> repository.count() == 0);
    }

    @Test
    void testSuccessfulProcessing() {
        // Send event
        kafkaTemplate.send(topic, createEvent(1L));
        // Assert Processing correct, stored in  DB
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Optional<BirthStatEntry> stored = repository.findById(1L);
            assertTrue(stored.isPresent());
            assertEquals(1L, stored.get().getId());
            assertEquals("Sparti", stored.get().getTown());
        });
    }

    @Test
    void testMissingId() {
        // Send event
        kafkaTemplate.send(topic, createEvent(null));
        // Assert Processing correct, not stored in  DB
        await().pollInterval(1, SECONDS)
                .atMost(3, SECONDS)
                .until(() -> repository.count() == 0);
    }

    @Test
    void testConstraintError() {
        // Send invalid event
        var event = createEvent(100L);
        event.setTown(null);
        event.setDob(LocalDate.now().plus(1, DAYS));
        kafkaTemplate.send(topic, event);

        //  Only the first one should be stored, the second should fail on unique constraint
        await().pollInterval(1, SECONDS)
                .atMost(3, SECONDS)
                .until(() -> repository.count() == 0);
    }

    @Test
    void nonTransientDbErrorIsNotReplayed() {
        // create invalid event that will fail validation (future dob and null town)
        var event = createEvent(200L);
        event.setTown(null);
        event.setDob(LocalDate.now().plusDays(1));

        // send event synchronously (no transaction required for tests)
        try {
            kafkaTemplate.send(topic, event).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // wait until the repository.saveAndFlush has been invoked once
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(dao, times(1)).saveAndFlush(any()));

        // ensure no additional invocations happen for a short stability window (no replay)
        Awaitility.await().during(500, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(dao, times(1)).saveAndFlush(any()));
    }

    @Test
    void transientDbErrorIsRetried() throws Exception {
        // simulate a transient DB error on first saveAndFlush and success on retry
        var event = createEvent(300L);
        AtomicInteger counter = new AtomicInteger();
        Answer<Object> answer = invocation -> {
            if (counter.getAndIncrement() == 0) {
                throw new org.springframework.dao.TransientDataAccessResourceException("simulated transient db error");
            }
            // on subsequent invocations call through to the real repository method
            return invocation.callRealMethod();
        };
        doAnswer(answer).when(dao).saveAndFlush(any());

        // send event synchronously to ensure immediate processing

        kafkaTemplate.send(topic, event).get();


        await().pollInterval(1, SECONDS)
                .atMost(5, SECONDS)
                .untilAsserted(() -> {
                    Optional<BirthStatEntry> stored = repository.findById(300L);
                    assertTrue(stored.isPresent());
                });

        // verify that saveAndFlush was attempted at least twice (initial failure + retry)
        verify(dao, org.mockito.Mockito.atLeast(2)).saveAndFlush(any());
    }

    @Test
    void testResolvedTestProperties() {
        // Ensure Spring resolved the embedded broker and mock schema registry for tests
        String bootstrap = env.getProperty("spring.kafka.bootstrap-servers");
        String schemaUrl = env.getProperty("spring.kafka.properties.schema.registry.url");
        String binderBrokers = env.getProperty("spring.cloud.stream.kafka.binder.brokers");
        String funcDef = env.getProperty("spring.cloud.function.definition");

        assertTrue(bootstrap != null && bootstrap.contains("127.0.0.1"), "bootstrap should point to embedded broker");
        assertEquals("mock://embedded-registry", schemaUrl);
        assertTrue(binderBrokers != null && binderBrokers.contains("127.0.0.1"));
        assertEquals("processBirthEvent", funcDef);
    }

    public static BirthEvent createEvent(Long id) {
        return BirthEvent.newBuilder()
                .setId(id)
                .setDob(LocalDate.now().minus(10, DAYS))
                .setName("John")
                .setTown("Sparti")
                .setWeight(BigDecimal.valueOf(3.1))
                .setRegistrationTime(Instant.now())
                .build();
    }


}
