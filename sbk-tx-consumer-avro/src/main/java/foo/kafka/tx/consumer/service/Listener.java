package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
@RequiredArgsConstructor
@Service
public class Listener {
    private final EntityManager entityManager;
    private final EventMapper eventMapper;

    @Transactional(transactionManager = "dbTM", propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    @KafkaListener(id = "${spring.kafka.consumer.group-id}", topics = "${spring.kafka.topic}")
    public void listen(BirthEvent event, ConsumerRecord<String, BirthEvent> message) {

        var entity = eventMapper.eventToEntity(event);
        log.info("[TX] Starting transaction for event: {} (topic={}, partition={}, offset={})", event, message.topic(), message.partition(), message.offset());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                switch (status) {
                    case STATUS_COMMITTED -> log.info("[TX] Transaction committed for event : {} stored as {} (topic={}, partition={}, offset={})", event, entity, message.topic(), message.partition(), message.offset());
                    case STATUS_ROLLED_BACK -> log.error("[TX] Transaction rolled back for event: {} (topic={}, partition={}, offset={})", event, message.topic(), message.partition(), message.offset());
                    default -> log.warn("[TX] Transaction status unknown for event: {} (topic={}, partition={}, offset={})", event, message.topic(), message.partition(), message.offset());
                }
            }
        });

        entityManager.persist(entity);
    }

}
