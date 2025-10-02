package foo.kafka.tx.consumer;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatEntry;
import foo.kafka.tx.consumer.persistence.BirthStatEntryRepository;
import foo.kafka.tx.consumer.service.EventMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ActiveProfiles("test")
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"birth.register.avro"},
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        }
)
class SbkTxConsumerApplicationTests {

    @TestConfiguration
    static class KafkaTestConfig {
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        @Bean
        public KafkaTemplate<String, BirthEvent> kafkaTemplate(Environment env) {
            Map<String, Object> props = new HashMap<>();
            props.put("bootstrap.servers", env.getProperty("spring.kafka.bootstrap-servers", bootstrapServers));
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("schema.registry.url", "mock://embedded-registry");
            props.put("specific.avro.reader", true);
            return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
        }
    }

    @Autowired
    private KafkaTemplate<String, BirthEvent> kafkaTemplate;

    @Autowired
    private EventMapper mapper;

    private final String topic = "birth.register.avro";

    @MockitoSpyBean
    BirthStatEntryRepository repository;


    @Autowired
    private EmbeddedKafkaBroker broker;

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
        // Assert Processing correct, stored in  DB
        await().pollInterval(1, SECONDS)
                .atMost(3, SECONDS)
                .until(() -> repository.count() == 0);
    }

    @Test
    void testConstraintError() {
        // Send invalid event
        var event = createEvent(100L);
        event.setTown(null);
        event.setDob(LocalDate.now().plus(1,DAYS));
        kafkaTemplate.send(topic, event);

        //  Only the first one should be stored, the second should fail on unique constraint
        await().pollInterval(1, SECONDS)
                .atMost(3, SECONDS)
                .until(() -> repository.count() == 0);


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
