package foo.kafka.tx.producer;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.producer.persistence.Birth;
import foo.kafka.tx.producer.persistence.BirthRepository;
import foo.kafka.tx.producer.service.BirthMapper;
import foo.kafka.tx.producer.service.ProcessorService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;

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
class SbkTxProducerApplicationTests {

    private String topic = "birth.register.avro";


    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private ProcessorService processor;

    @Autowired
    private BirthMapper mapper;

    @Autowired
    private BirthRepository repository;

    @MockitoBean
    private BirthRepository mockRepository;


    private Consumer<String, BirthEvent> consumer;

    private Consumer<String, BirthEvent> createConsumer() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.consumerProps("test-group", "true", broker));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", "mock://embedded-registry");
        props.put("specific.avro.reader", true);
        return new DefaultKafkaConsumerFactory<String, BirthEvent>(props).createConsumer();
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) consumer.close(Duration.ofSeconds(1));
    }

    @Test
    void sendsTransactionalSpecificRecord_withMockRegistry() {
        consumer = createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, topic);

        Birth entity = new Birth();
        entity.setId(1L);
        entity.setDob(LocalDate.now());
        entity.setName("John");
        entity.setTown("Sparti");
        entity.setWeight(BigDecimal.valueOf(3.1));
        entity.setRegistrationTime(Instant.now());

        BirthEvent event = mapper.toBirthEvent(entity);


        processor.sendAndStore(entity);

        // Assert Kafka
        var record = KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(5));
        BirthEvent value = record.value();
        assertNotNull(value);
        assertNotNull(value); // String may be Utf8
        assertNotNull(value.getDob()); // String may be Utf8
        assertEquals("Sparti", value.getTown()); // String may be Utf8
        assertEquals("John", value.getName()); // String may be Utf8

        // Assert DB using Awaitility
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Optional<Birth> stored = repository.findById(1L);
            assertTrue(stored.isPresent());
            assertEquals("John", stored.get().getName());
            assertEquals("Sparti", stored.get().getTown());
        });
    }

    @Test
    void doesNotSendKafkaMessage_whenDbErrorOccurs() {
        consumer = createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, topic);

        Birth entity = new Birth();
        entity.setId(2L);
        entity.setDob(LocalDate.now());
        entity.setName("Error");
        entity.setTown("FailTown");
        entity.setWeight(BigDecimal.valueOf(2.5));
        entity.setRegistrationTime(Instant.now());

        // Simulate DB error
        doThrow(new RuntimeException("DB error")).when(mockRepository).saveAndFlush(entity);

        // Call service method
        try {
            processor.sendAndStore(entity);
        } catch (Exception ignored) {}

        // Assert no Kafka message sent
        await().during(Duration.ofSeconds(3)).atMost(5, SECONDS).untilAsserted(() -> {
            assertThrows(Exception.class, () -> KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(1)));
        });
    }
}
