package foo.kafka.tx.producer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.producer.persistence.Birth;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaTemplate<String, BirthEvent> birthEventKafkaTemplate;
    private final BirthMapper birthMapper;
    private static final String TOPIC = "birth.register";

    @Transactional(transactionManager = "kafkaTM")
    public Birth sendKafka(Birth birth) {
        BirthEvent birthEvent = birthMapper.toBirthEvent(birth);
        birthEventKafkaTemplate.send(TOPIC, birthEvent);
        log.info("[TX][{}] send completed for: {}", birthEvent);
        return birth;
    }

}
