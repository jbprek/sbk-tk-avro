package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatEntryRepository;
import jakarta.validation.ConstraintViolation;
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
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


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

        AtomicBoolean skipOnRollback = new AtomicBoolean(false);

        try {
            repository.saveAndFlush(entity);
            // do not acknowledge here; acknowledge only after transaction commits
            log.debug("[TX] Save successful inside transaction for event at: {}", messageInfo);
        } catch (Exception ex) {
            log.error("[TX] Exception during DB save for event at : {}, error: {} )", messageInfo, ex.getMessage(), ex);
            if (isNonTransientDbError(ex)) {
                Throwable root = getRootCause(ex);
                String constraintDetails = formatConstraintViolations(root);
                if (constraintDetails != null) {
                    log.warn("[TX] Non-transient DB validation error ({}), will skip message after transaction completes: {}",
                            constraintDetails, messageInfo);
                } else {
                    log.warn("[TX] Non-transient DB error (root cause: {}), will skip message after transaction completes: {}",
                            root != null ? root.getClass().getName() + ": " + root.getMessage() : ex.getClass().getName(),
                            messageInfo);
                }
                // mark that we want to skip (ack) even though the DB transaction will be rolled back
                skipOnRollback.set(true);
                // ensure current transaction is marked rollback so that DB changes are rolled back
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                // swallow the exception so the container won't treat it as an error (we'll ack after rollback)
            } else {
                throw ex; // let Kafka retry by throwing
            }
        }

        registerTransactionSynchronization(messageInfo, event, entity, acknowledgment, skipOnRollback);
    }

    private void registerTransactionSynchronization(String messageInfo, BirthEvent event, Object entity, Acknowledgment acknowledgment, AtomicBoolean skipOnRollback) {
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
                    case STATUS_ROLLED_BACK -> {
                        log.info("[TX] Transaction rolled back for event at {} ", messageInfo);
                        if (skipOnRollback.get()) {
                            try {
                                acknowledgment.acknowledge();
                                log.warn("[TX] Acknowledged (skipped) message after rollback at: {}", messageInfo);
                            } catch (Exception e) {
                                log.error("[TX] Failed to acknowledge skipped message {}: {}", messageInfo, e.getMessage(), e);
                            }
                        } else {
                            log.debug("[TX] Not acknowledging; message will be retried: {}", messageInfo);
                        }
                    }
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

    private String formatConstraintViolations(Throwable ex) {
        // find the nearest ConstraintViolationException in the cause chain
        Throwable cur = ex;
        while (cur != null) {
            if (cur instanceof ConstraintViolationException cve) {
                if (cve.getConstraintViolations() == null || cve.getConstraintViolations().isEmpty()) {
                    return "constraint violations: none";
                }
                return cve.getConstraintViolations().stream()
                        .map(v -> v.getPropertyPath() + "=" + v.getMessage())
                        .collect(Collectors.joining(", "));
            }
            cur = cur.getCause();
        }
        return null;
    }

}
