package foo.kafka.tx.producer;

import foo.avro.birth.BirthEvent;
import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    private final Environment env;

    public KafkaConfig(Environment env) {
        this.env = env;
    }

    @Value("${spring.kafka.producer.transaction-id-prefix}")
    private String transactionIdPrefix;

    @Bean
    public ProducerFactory<String, BirthEvent> birthEventProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers"));
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, env.getProperty("spring.kafka.producer.key-serializer"));
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, env.getProperty("spring.kafka.producer.value-serializer"));
        configProps.put("schema.registry.url", env.getProperty("spring.kafka.producer.properties.schema.registry.url"));
        configProps.put("acks", env.getProperty("spring.kafka.producer.properties.acks", "all"));
        configProps.put("enable.idempotence", env.getProperty("spring.kafka.producer.properties.enable.idempotence", "true"));
        DefaultKafkaProducerFactory<String, BirthEvent> factory = new DefaultKafkaProducerFactory<>(configProps);
        factory.setTransactionIdPrefix(transactionIdPrefix);
        return factory;
    }

    @Bean
    public KafkaTemplate<String, BirthEvent> birthEventKafkaTemplate() {
        return new KafkaTemplate<>(birthEventProducerFactory());
    }

    @Bean(name="kafkaTM")
    public KafkaTransactionManager<String, BirthEvent> kafkaTxManager(ProducerFactory<String, BirthEvent> pf) {
        KafkaTransactionManager<String, BirthEvent> ktm = new KafkaTransactionManager<>(pf);
        ktm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
        return ktm;
    }

    @Bean(name={"dbTM", "transactionManager"})
    public PlatformTransactionManager jpaTransactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }
}
