package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatDao;
import foo.kafka.tx.consumer.persistence.BirthStatEntry;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

/**
 * Tests for Processor; helpers in Processor are package-private so tests can call them directly.
 */
class ProcessorTest {

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void process_committed_withAcknowledgment() throws Exception {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        Processor processor = new Processor(mapper, repository);

        BirthEvent payload = mock(BirthEvent.class);
        BirthStatEntry entity = new BirthStatEntry();

        when(mapper.eventToEntity(payload)).thenReturn(entity);
        when(repository.saveAndFlush(entity)).thenReturn(entity);

        Acknowledgment ack = mock(Acknowledgment.class);

        Message<BirthEvent> message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "my-topic")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 2)
                .setHeader(KafkaHeaders.OFFSET, 7L)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .build();

        TransactionSynchronizationManager.initSynchronization();
        try {
            processor.process(message);

            List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
            assertFalse(syncs.isEmpty(), "expected at least one registered synchronization");

            // simulate successful commit
            syncs.get(0).afterCompletion(TransactionSynchronization.STATUS_COMMITTED);

            verify(ack, times(1)).acknowledge();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void process_committed_withManualConsumerCommit() throws Exception {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        Processor processor = new Processor(mapper, repository);

        BirthEvent payload = mock(BirthEvent.class);
        BirthStatEntry entity = new BirthStatEntry();

        when(mapper.eventToEntity(payload)).thenReturn(entity);
        when(repository.saveAndFlush(entity)).thenReturn(entity);

        @SuppressWarnings("unchecked")
        Consumer<String, Object> consumer = mock(Consumer.class);

        Message<BirthEvent> message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "topic-manual")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 5)
                .setHeader(KafkaHeaders.OFFSET, 11L)
                .setHeader(KafkaHeaders.CONSUMER, consumer)
                .build();

        TransactionSynchronizationManager.initSynchronization();
        try {
            processor.process(message);

            List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
            assertFalse(syncs.isEmpty());

            syncs.get(0).afterCompletion(TransactionSynchronization.STATUS_COMMITTED);

            // consumer.commitSync should be called with a map containing the TopicPartition and offset+1
            verify(consumer, times(1)).commitSync(anyMap());
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void registerTransactionSynchronization_rollback_skippedAcknowledged() throws Exception {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        Processor processor = new Processor(mapper, repository);

        BirthEvent payload = mock(BirthEvent.class);
        BirthStatEntry entity = new BirthStatEntry();

        Acknowledgment ack = mock(Acknowledgment.class);
        @SuppressWarnings("unchecked")
        Consumer<String, Object> consumer = mock(Consumer.class);

        Processor.AckInfo ackInfo = new Processor.AckInfo(ack, consumer, "r-topic", 3, 42L, "msg-info");

        AtomicBoolean skipOnRollback = new AtomicBoolean(true);

        // Make sure transaction synchronization is active so registerSynchronization works
        TransactionSynchronizationManager.initSynchronization();
        try {
            // call the package-private registerTransactionSynchronization method directly
            processor.registerTransactionSynchronization(ackInfo, payload, entity, skipOnRollback);

            List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
            assertFalse(syncs.isEmpty());

            // simulate rollback
            syncs.get(0).afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);

            // because skipOnRollback was true we should acknowledge/commit (acknowledgment path used)
            verify(ack, times(1)).acknowledge();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void acknowledgeOrCommit_withAcknowledgment_invokesAck() {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        Processor processor = new Processor(mapper, repository);

        Acknowledgment ack = mock(Acknowledgment.class);
        Processor.AckInfo ackInfo = new Processor.AckInfo(ack, null, "t", 1, 2L, "m");

        processor.acknowledgeOrCommit(ackInfo, false, "test");

        verify(ack, times(1)).acknowledge();
    }

    @Test
    void acknowledgeOrCommit_withConsumer_commitsOffset() {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        Processor processor = new Processor(mapper, repository);

        @SuppressWarnings("unchecked")
        Consumer<String, Object> consumer = mock(Consumer.class);
        Processor.AckInfo ackInfo = new Processor.AckInfo(null, consumer, "topic-x", 6, 100L, "m");

        processor.acknowledgeOrCommit(ackInfo, false, "test");

        verify(consumer, times(1)).commitSync(anyMap());
    }

    @Test
    void isNonTransientDbError_and_related_helpers() {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        Processor processor = new Processor(mapper, repository);

        // isNonTransientDbError should detect IllegalArgumentException
        Throwable t = new IllegalArgumentException("bad");
        assertTrue(processor.isNonTransientDbError(t));

        // getRootCause should traverse causes
        Throwable root = new RuntimeException("root");
        Throwable outer = new Exception("outer", root);
        assertSame(root, processor.getRootCause(outer));

        // formatConstraintViolations should handle an empty ConstraintViolationException
        ConstraintViolationException cve = new ConstraintViolationException("msg", Collections.emptySet());
        assertEquals("constraint violations: none", processor.formatConstraintViolations(cve));
    }

    @Test
    void process_nonTransientDbError_swallowed_and_skipped_onRollback() throws Exception {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        // create a mock TransactionStatus and a Processor test-subclass that delegates the rollback-only call to the mock
        final TransactionStatus tx = mock(TransactionStatus.class);
        Processor processor = new Processor(mapper, repository) {
            @Override
            void markCurrentTransactionRollbackOnly() {
                tx.setRollbackOnly();
            }
        };

        BirthEvent payload = mock(BirthEvent.class);
        BirthStatEntry entity = new BirthStatEntry();

        when(mapper.eventToEntity(payload)).thenReturn(entity);
        // simulate non-transient DB error thrown during save
        when(repository.saveAndFlush(entity)).thenThrow(new IllegalArgumentException("non-transient"));

        Acknowledgment ack = mock(Acknowledgment.class);

        Message<BirthEvent> message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "my-topic")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 4)
                .setHeader(KafkaHeaders.OFFSET, 21L)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .build();

        TransactionSynchronizationManager.initSynchronization();
        try {
            // process should swallow the non-transient exception and continue
            processor.process(message);

            List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
            assertFalse(syncs.isEmpty());

            // simulate rollback -> since skipOnRollback was set, ack should be called
            syncs.get(0).afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
            verify(ack, times(1)).acknowledge();

            // ensure rollbackOnly was set on the mock transaction
            verify(tx, times(1)).setRollbackOnly();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void process_transientError_rethrown() {
        EventMapper mapper = mock(EventMapper.class);
        BirthStatDao repository = mock(BirthStatDao.class);
        Processor processor = new Processor(mapper, repository);

        BirthEvent payload = mock(BirthEvent.class);
        BirthStatEntry entity = new BirthStatEntry();

        when(mapper.eventToEntity(payload)).thenReturn(entity);
        // simulate a transient error that should be rethrown (RuntimeException not considered non-transient)
        when(repository.saveAndFlush(entity)).thenThrow(new RuntimeException("transient"));

        Message<BirthEvent> message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "my-topic")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 9)
                .setHeader(KafkaHeaders.OFFSET, 99L)
                .build();

        assertThrows(RuntimeException.class, () -> processor.process(message));
    }
}
