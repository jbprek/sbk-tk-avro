package foo.kafka.simple.producer;

import foo.avro.birth.BirthEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    private final Environment env;

    public KafkaConfig(Environment env) {
        this.env = env;
    }

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
        return factory;
    }

    @Bean
    public KafkaTemplate<String, BirthEvent> birthEventKafkaTemplate() {
        return new KafkaTemplate<>(birthEventProducerFactory());
    }


}
