package foo.kafka.tx.producer.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface BirthRepository extends JpaRepository<Birth, Long> {
}