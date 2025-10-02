package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;

import foo.kafka.tx.consumer.persistence.BirthStats;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import jakarta.persistence.EntityManager;
import java.time.LocalDate;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
class ProcessorIntegrationTest {
    public static final LocalDate TEST_DT = LocalDate.parse("2025-05-20");
    @Autowired
    private Processor processor;
    @Autowired
    private EntityManager entityManager;
    @Autowired
    private EventMapper eventMapper;

    @Test
    void testProcessPersistsEntity() {
        BirthEvent event = BirthEvent.newBuilder()
            .setId(123L)
            .setName("Jane Doe")
            .setDob(TEST_DT)
            .setTown("Athens")
            .build();
        var message = MessageBuilder.withPayload(event)
            .copyHeaders(Map.of(
                KafkaHeaders.RECEIVED_TOPIC, "birth-events",
                KafkaHeaders.RECEIVED_PARTITION, 0,
                KafkaHeaders.OFFSET, 1L
            ))
            .build();
        processor.process(message);
        var result = entityManager.createQuery("SELECT b FROM BirthStats b WHERE b.name = :name", BirthStats.class)
            .setParameter("name", "Jane Doe")
            .getSingleResult();
        assertThat(result).isNotNull();
        assertThat(result.getDob()).isEqualTo(TEST_DT);
        assertThat(result.getTown()).isEqualTo("Testville");
    }
}

