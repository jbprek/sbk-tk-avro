package foo.kafka.tx.consumer.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface BirthStatEntryRepository extends JpaRepository<BirthStatEntry, Long> {
}