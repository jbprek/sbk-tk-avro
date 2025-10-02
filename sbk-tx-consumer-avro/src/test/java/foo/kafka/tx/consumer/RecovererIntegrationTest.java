package foo.kafka.tx.consumer;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatEntryRepository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@ActiveProfiles("test")
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"birth.register.avro"})
class RecovererIntegrationTest {

    @Autowired
    private KafkaTemplate<String, BirthEvent> kafkaTemplate;

    @MockitoSpyBean
    private BirthStatEntryRepository repository;

    @Test
    void nonTransientDbErrorIsNotReplayed() throws Exception {
        // create invalid event that will fail validation (future dob and null town)
        var event = SbkTxConsumerApplicationTests.createEvent(200L);
        event.setTown(null);
        event.setDob(LocalDate.now().plusDays(1));

        // send event
        // send within a producer transaction to satisfy transactional KafkaTemplate configurations
        kafkaTemplate.executeInTransaction(k -> {
            try {
                k.send("birth.register.avro", event).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        // wait until the repository.saveAndFlush has been invoked once
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(repository, times(1)).saveAndFlush(any()));

        // ensure no additional invocations happen for a short stability window (no replay)
        Awaitility.await().during(500, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(repository, times(1)).saveAndFlush(any()));
    }
}
