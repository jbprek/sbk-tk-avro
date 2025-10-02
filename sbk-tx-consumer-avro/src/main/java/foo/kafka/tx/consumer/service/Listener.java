package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatEntryRepository;
import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.TransactionSystemException;
import jakarta.persistence.PersistenceException;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;


@Slf4j
@RequiredArgsConstructor
@Service
public class Listener {
    private final BirthStatEntryRepository repository;
    private final EventMapper eventMapper;

    @Transactional(
            transactionManager = "dbTM",
            propagation = Propagation.REQUIRES_NEW,
            rollbackFor = {Exception.class}
    )
    @KafkaListener(id = "${spring.kafka.consumer.group-id}", topics = "${spring.kafka.topic}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(BirthEvent event, ConsumerRecord<String, BirthEvent> message, Acknowledgment acknowledgment) {
        String messageInfo = KafkaHelper.getRecordInfo(message);
        var entity = eventMapper.eventToEntity(event);
        log.info("[TX] Starting transaction for event at: {} : {}", messageInfo, event);

        try {
            repository.saveAndFlush(entity);
            // do not acknowledge here; acknowledge only after transaction commits
            log.debug("[TX] Save successful inside transaction for event at: {}", messageInfo);
        } catch (Exception ex) {
            log.error("[TX] Exception during DB save for event at : {}, error: {} )", messageInfo, ex.getMessage(), ex);
            if (isNonTransientDbError(ex)) {
                Throwable root = getRootCause(ex);
                log.warn("[TX] Non-transient DB error (root cause: {}), delegating to non-retryable recoverer for message: {}",
                        root != null ? root.getClass().getName() + ": " + root.getMessage() : ex.getClass().getName(),
                        messageInfo);
                // Throw a custom non-retryable exception so the DefaultErrorHandler will invoke the recoverer (which commits offset)
                throw new NonRetryableProcessingException("Non-transient DB error while processing message: " + messageInfo, ex);
            }
            // rethrow transient exceptions so the DefaultErrorHandler will retry
            throw ex;
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                switch (status) {
                    case STATUS_COMMITTED -> {
                        log.info("[TX] Transaction committed for event at: {} ", messageInfo);
                        // only acknowledge after successful DB commit
                        try {
                            acknowledgment.acknowledge();
                            log.debug("[TX] Acknowledged message after commit: {}", messageInfo);
                        } catch (Exception e) {
                            log.error("[TX] Failed to acknowledge after commit for {}: {}", messageInfo, e.getMessage(), e);
                        }
                        log.debug("[TX] Commited details for event at {} : {}  stored as {} ", messageInfo, event, entity);
                    }
                    case STATUS_ROLLED_BACK -> log.info("[TX] Transaction rolled back for event at {} ", messageInfo);
                    default -> log.warn("[TX] Transaction status unknown for event at: {} ", messageInfo);
                }
            }
        });
    }

    // walk cause chain to detect DB exceptions that are non-transient and should be skipped
    private boolean isNonTransientDbError(Throwable ex) {
        while (ex != null) {
            if (ex instanceof JpaSystemException
                    || ex instanceof DataIntegrityViolationException
                    || ex instanceof ConstraintViolationException
                    || ex instanceof IllegalArgumentException
                    || ex instanceof org.springframework.transaction.TransactionSystemException
                    || ex instanceof jakarta.persistence.PersistenceException) {
                return true;
            }
            ex = ex.getCause();
        }
        return false;
    }

    private Throwable getRootCause(Throwable ex) {
        Throwable root = ex;
        while (root != null && root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }
}
