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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

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

    @MockitoSpyBean
    private BirthRepository repository;

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

        Birth entity = createTestBirth(1L);

        processor.sendAndStore(entity);

        var expectedEvent = mapper.toBirthEvent(entity);

        // Assert Kafka
        var record = KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(5));
        var value = record.value();
        assertNotNull(value);
        assertNotNull(value); // String may be Utf8
        assertNotNull(value.getDob()); // String may be Utf8
        assertEquals(expectedEvent.getTown(), value.getTown()); // String may be Utf8
        assertEquals(expectedEvent.getName(), value.getName()); // String may be Utf8

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

        Birth entity = createTestBirth(2L);

        // Simulate DB error
        doThrow(new RuntimeException("DB error")).when(repository).saveAndFlush(entity);

        // Call service method
        try {
            processor.sendAndStore(entity);
        } catch (Exception ignored) {
            // Exception is expected due to simulated DB error
        }

        // Assert no Kafka message sent
        await().during(Duration.ofSeconds(3)).atMost(5, SECONDS).untilAsserted(() -> {
            assertThrows(Exception.class, () -> KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(1)));
        });
    }

    private static Birth createTestBirth(Long id) {
        Birth entity = new Birth();
        entity.setId(id);
        entity.setDob(LocalDate.now());
        entity.setName("John");
        entity.setTown("Sparti");
        entity.setWeight(BigDecimal.valueOf(3.1));
        entity.setRegistrationTime(Instant.now());
        return entity;
    }
}
