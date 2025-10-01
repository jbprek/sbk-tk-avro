package foo.kafka.tx.producer.service;

import foo.kafka.tx.producer.persistence.Birth;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
@Service
public class ProcessorService {

    private final KafkaProducerService kafkaProducerService;
    private final BirthDaoService birthDaoService;
    private final BirthMapper mapper;

    @Transactional(transactionManager = "dsTM")
    public Birth sendAndStore(Birth birth) {
        log.info("Processing birth: {}", birth);
        birthDaoService.persist(birth);
        var event = mapper.toBirthEvent(birth);
        kafkaProducerService.sendKafka(event);
        return birth;
    }
}
