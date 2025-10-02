package foo.kafka.tx.consumer;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.service.KafkaHelper;
import foo.kafka.tx.consumer.service.NonRetryableProcessingException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConfig {

    public static final long MAX_ATTEMPTS = 2L;
    @Value("${spring.kafka.bootstrap-servers}")
    private  String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ConsumerFactory<String, BirthEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        // disable auto commit so we control commits via ack or recoverer
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("specific.avro.reader", true);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        // Consumer-aware recoverer commits the offset for the failed record when error is non-retryable
        ConsumerAwareRecordRecoverer recoverer = (rec, consumer, ex) -> {
            var messageInfo = KafkaHelper.getRecordInfo(rec);
            Object value = rec.value();
            log.error("[TX] Error processing record (recoverer): {}, value={}, exception={}, type={}, cause={}",
                    messageInfo, value, ex == null ? "<null>" : ex.getMessage(), ex == null ? "<null>" : ex.getClass().getName(),
                    ex != null && ex.getCause() != null ? ex.getCause().getClass().getName() : "null", ex);
            // commit the offset so the record is not replayed (commit next offset)
            TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
            OffsetAndMetadata oam = new OffsetAndMetadata(rec.offset() + 1);
            try {
                consumer.commitSync(Collections.singletonMap(tp, oam));
                log.warn("[TX] Committed offset for skipped record: {}", messageInfo);
            } catch (Exception commitEx) {
                log.error("[TX] Failed to commit offset for skipped record {}: {}", messageInfo, commitEx.getMessage(), commitEx);
            }
            // You can add alerting logic here, e.g., send an email, push notification, etc.
        };
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, MAX_ATTEMPTS));
        errorHandler.setRetryListeners((consumerRecord, ex, deliveryAttempt) -> {
            if (deliveryAttempt > MAX_ATTEMPTS) {
                log.warn("[TX] Retry exhausted: Record failed after {} attempts: topic={}, partition={}, offset={}, key={}, value={}",
                        deliveryAttempt, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                // You can add further alerting logic here
            } else {
                // Avoid logging attempt 1 (initial failure) to reduce noise; only log actual retry attempts > 1
                if (deliveryAttempt > 1) {
                    log.warn("[TX] Retry attempt {} for record: topic={}, partition={}, offset={}, key={}, value={}",
                            deliveryAttempt, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                }
            }
        });
        errorHandler.addNotRetryableExceptions(
            org.springframework.orm.jpa.JpaSystemException.class,
            org.springframework.dao.DataIntegrityViolationException.class,
            jakarta.validation.ConstraintViolationException.class,
            IllegalArgumentException.class,
            org.springframework.transaction.TransactionSystemException.class,
            jakarta.persistence.PersistenceException.class,
            NonRetryableProcessingException.class
        );
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, BirthEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, BirthEvent> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, BirthEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    @Bean(name={"dbTM", "transactionManager"})
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }

}
