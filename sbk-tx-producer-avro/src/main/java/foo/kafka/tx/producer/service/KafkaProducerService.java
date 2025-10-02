package foo.kafka.tx.producer.service;

import foo.avro.birth.BirthEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaTemplate<String, BirthEvent> birthEventKafkaTemplate;

    @Value("${spring.kafka.template.default-topic}")
    private String topic;

    @Transactional(transactionManager = "kafkaTM")
    public BirthEvent sendKafka(BirthEvent birth) {
        birthEventKafkaTemplate.send(topic, birth);
        log.info("[TX][{}] send completed for: {}", birth);
        return birth;
    }

}
