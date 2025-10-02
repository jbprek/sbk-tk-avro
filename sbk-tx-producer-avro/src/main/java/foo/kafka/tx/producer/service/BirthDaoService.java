package foo.kafka.tx.producer.service;

import foo.kafka.tx.producer.persistence.Birth;
import foo.kafka.tx.producer.persistence.BirthRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class BirthDaoService {

    private final BirthRepository repository;


    @Transactional(transactionManager = "dsTM")
    public Birth persist(Birth birth) {
        try {
            repository.saveAndFlush(birth);

            log.info("[TX][{}] Entity persisted: {}",  birth);
        } catch (Exception e) {
            log.error("[TX][{}] DB persist failed, transaction will be rolled back: {}",  e.getMessage());
            throw e;
        }
        return birth;
    }
}
