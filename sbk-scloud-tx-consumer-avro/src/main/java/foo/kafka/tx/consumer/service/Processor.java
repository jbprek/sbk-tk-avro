package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatDao;
import foo.kafka.tx.consumer.persistence.BirthStatEntryRepository;
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
    private final EventMapper eventMapper;
    private final BirthStatDao repository;

    // small holder to reduce long parameter lists passed around
    private record AckInfo(
            Acknowledgment acknowledgment,
            Consumer<?, ?> consumer,
            String topic,
            Integer partition,
            Long offset,
            String messageInfo
    ) {}

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

        AckInfo ackInfo = new AckInfo(acknowledgment, consumer, topic, partition, offset, messageInfo);

        registerTransactionSynchronization(ackInfo, event.getPayload(), entity, skipOnRollback);
    }

    private void registerTransactionSynchronization(AckInfo ackInfo, BirthEvent event, Object entity, AtomicBoolean skipOnRollback) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                switch (status) {
                    case STATUS_COMMITTED:
                        log.info("[TX] Transaction committed for event at: {} ", ackInfo.messageInfo());
                        // only acknowledge after successful DB commit
                        acknowledgeOrCommit(ackInfo, false, "after commit");
                        log.debug("[TX] Commited details for event at {} : {}  stored as {} ", ackInfo.messageInfo(), event, entity);
                        break;
                    case STATUS_ROLLED_BACK:
                        log.info("[TX] Transaction rolled back for event at {} ", ackInfo.messageInfo());
                        if (skipOnRollback.get()) {
                            // we want to skip this message even though the DB transaction was rolled back
                            acknowledgeOrCommit(ackInfo, true, "skipped after rollback");
                        } else {
                            log.debug("[TX] Not acknowledging; message will be retried: {}", ackInfo.messageInfo());
                        }
                        break;
                    default:
                        log.warn("[TX] Transaction status unknown for event at: {} ", ackInfo.messageInfo());
                }
            }
        });
    }

    // helper that centralizes acknowledgment vs manual offset commit logic
    // Suppress resource warning: the Kafka Consumer is managed by the container and must not be closed here.
    @SuppressWarnings("resource")
    private void acknowledgeOrCommit(AckInfo ackInfo, boolean skipMode, String action) {
        try {
            if (ackInfo.acknowledgment() != null) {
                ackInfo.acknowledgment().acknowledge();
                if (skipMode) {
                    log.warn("[TX] Acknowledged (skipped) message {} at: {}", action, ackInfo.messageInfo());
                } else {
                    log.debug("[TX] Acknowledged message {}: {}", action, ackInfo.messageInfo());
                }
            } else if (ackInfo.consumer() != null && ackInfo.partition() != null && ackInfo.offset() != null) {
                // extract manual commit to its own method to avoid nested try blocks
                commitOffset(ackInfo.consumer(), ackInfo.topic(), ackInfo.partition(), ackInfo.offset(), action, ackInfo.messageInfo(), skipMode);
            } else {
                log.warn("[TX] No acknowledgment or consumer available to {} for: {}", action, ackInfo.messageInfo());
            }
        } catch (Exception e) {
            log.error("[TX] Failed to acknowledge/commit {} for {}: {}", action, ackInfo.messageInfo(), e.getMessage(), e);
        }
    }

    private void commitOffset(Consumer<?, ?> consumer, String topic, Integer partition, Long offset, String action, String messageInfo, boolean skipMode) {
        try {
            TopicPartition tp = new TopicPartition(topic, partition);
            Map<TopicPartition, OffsetAndMetadata> commit = Collections.singletonMap(tp, new OffsetAndMetadata(offset + 1));
            consumer.commitSync(commit);
            if (skipMode) {
                log.warn("[TX] Manually committed offset (skipped) {} at: {} -> {}", action, messageInfo, commit);
            } else {
                log.debug("[TX] Manually committed offset {} at: {} -> {}", action, messageInfo, commit);
            }
        } catch (Exception e) {
            log.error("[TX] Failed to manually commit offset {} for {}: {}", action, messageInfo, e.getMessage(), e);
        }
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
