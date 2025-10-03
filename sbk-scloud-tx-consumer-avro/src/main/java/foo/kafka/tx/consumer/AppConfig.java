package foo.kafka.tx.consumer;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.service.Processor;
import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.function.Consumer;

@Slf4j
@Configuration
public class AppConfig {

    @Bean(name = {"dbTM", "transactionManager"})
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }

    @Bean
    public Consumer<Message<BirthEvent>> processBirthEvent(Processor processor) {
        // use lambda to ensure proxy invocation and AOP (@Transactional) are applied correctly
        return message -> processor.process(message);
    }
}
