package foo.kafka.tx.producer.service;

import foo.kafka.tx.producer.persistence.Birth;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class BirthDaoService {
    @PersistenceContext
    private EntityManager entityManager;

    @Transactional(transactionManager = "dbTM")
    public Birth persist(Birth birth) {
        try {
            entityManager.persist(birth);

            log.info("[TX][{}] Entity persisted: {}",  birth);
        } catch (Exception e) {
            log.error("[TX][{}] DB persist failed, transaction will be rolled back: {}",  e.getMessage());
            throw e;
        }
        return birth;
    }
}
