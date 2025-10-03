package foo.kafka.tx.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KafkaConfig {

    // Configuration for the Kafka binder is provided via application.yml under spring.cloud.stream
    // We don't create Spring-Kafka ConsumerFactory / ListenerContainer beans here because
    // this application uses Spring Cloud Stream functional programming model with native decoding.

}
