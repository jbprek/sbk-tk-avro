package foo.kafka.simple.producer.service;

import foo.avro.birth.BirthEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaTemplate<String, BirthEvent> birthEventKafkaTemplate;

    @Value("${spring.kafka.topic}")
    private String topic;

    public void sendKafka(BirthEvent birth) {
        birthEventKafkaTemplate.send(topic, birth);
        log.info("[TX]send completed for: {}", birth);
    }

}


