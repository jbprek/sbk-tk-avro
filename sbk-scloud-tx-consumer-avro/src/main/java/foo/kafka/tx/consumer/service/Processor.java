package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatEntryRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class Processor {
    private final EntityManager entityManager;
    private final EventMapper eventMapper;
    private final BirthStatEntryRepository repository;


    @Transactional(
            transactionManager = "dbTM",
            propagation = Propagation.REQUIRES_NEW,
            rollbackFor = {Exception.class}
    )
    public void process(Message<BirthEvent> event) {
        // functional signature used by Spring Cloud Stream -- extract Kafka details from headers
        String topic = (String) event.getHeaders().get(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC);
        Integer partition = (Integer) event.getHeaders().get(org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION);
        Long offset = (Long) event.getHeaders().get(org.springframework.kafka.support.KafkaHeaders.OFFSET);

        String messageInfo = "topic %s:(%s,%s)".formatted(topic, partition, offset);

        log.info("[TX] Starting transaction for event at: {} : {}", messageInfo, event.getPayload());

        var entity = eventMapper.eventToEntity(event.getPayload());

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
                throw ex; // let binder/Kafka retry by throwing
            }
        }

        // get acknowledgment from headers (available when using native decoding/manual ack)
        Acknowledgment acknowledgment = event.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        Consumer<?, ?> consumer = event.getHeaders().get(KafkaHeaders.CONSUMER, Consumer.class);

        registerTransactionSynchronization(topic, messageInfo, event.getPayload(), entity, acknowledgment, consumer, partition, offset, skipOnRollback);
    }

    private void registerTransactionSynchronization(String topic, String messageInfo, BirthEvent event, Object entity, Acknowledgment acknowledgment, Consumer<?, ?> consumer, Integer partition, Long offset, AtomicBoolean skipOnRollback) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                switch (status) {
                    case STATUS_COMMITTED -> {
                        log.info("[TX] Transaction committed for event at: {} ", messageInfo);
                        // only acknowledge after successful DB commit
                        try {
                            if (acknowledgment != null) {
                                acknowledgment.acknowledge();
                                log.debug("[TX] Acknowledged message after commit: {}", messageInfo);
                            } else if (consumer != null && partition != null && offset != null) {
                                // commit offset manually using the provided topic
                                try {
                                    TopicPartition tp = new TopicPartition(topic, partition);
                                    Map<TopicPartition, OffsetAndMetadata> commit = Collections.singletonMap(tp, new OffsetAndMetadata(offset + 1));
                                    consumer.commitSync(commit);
                                    log.debug("[TX] Manually committed offset after commit at: {} -> {}", messageInfo, commit);
                                } catch (Exception e) {
                                    log.error("[TX] Failed to manually commit offset after commit for {}: {}", messageInfo, e.getMessage(), e);
                                }
                            } else {
                                log.warn("[TX] No acknowledgment or consumer available to commit for: {}", messageInfo);
                            }
                        } catch (Exception e) {
                            log.error("[TX] Failed to acknowledge after commit for {}: {}", messageInfo, e.getMessage(), e);
                        }
                        log.debug("[TX] Commited details for event at {} : {}  stored as {} ", messageInfo, event, entity);
                    }
                    case STATUS_ROLLED_BACK -> {
                        log.info("[TX] Transaction rolled back for event at {} ", messageInfo);
                        if (skipOnRollback.get()) {
                            try {
                                if (acknowledgment != null) {
                                    acknowledgment.acknowledge();
                                    log.warn("[TX] Acknowledged (skipped) message after rollback at: {}", messageInfo);
                                } else if (consumer != null && partition != null && offset != null) {
                                    try {
                                        TopicPartition tp = new TopicPartition(topic, partition);
                                        Map<TopicPartition, OffsetAndMetadata> commit = Collections.singletonMap(tp, new OffsetAndMetadata(offset + 1));
                                        consumer.commitSync(commit);
                                        log.warn("[TX] Manually committed offset (skipped) after rollback at: {} -> {}", messageInfo, commit);
                                    } catch (Exception e) {
                                        log.error("[TX] Failed to manually commit skipped message {}: {}", messageInfo, e.getMessage(), e);
                                    }
                                } else {
                                    log.warn("[TX] No acknowledgment or consumer available to skip for: {}", messageInfo);
                                }
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
            if (ex instanceof org.springframework.orm.jpa.JpaSystemException
                    || ex instanceof DataIntegrityViolationException
                    || ex instanceof ConstraintViolationException
                    || ex instanceof IllegalArgumentException
                    || ex instanceof TransactionSystemException
                    || ex instanceof PersistenceException) {
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
