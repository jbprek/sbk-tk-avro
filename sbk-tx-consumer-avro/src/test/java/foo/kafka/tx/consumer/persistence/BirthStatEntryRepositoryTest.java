package foo.kafka.tx.consumer.persistence;

import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@ActiveProfiles("test")
@DataJpaTest
class BirthStatEntryRepositoryTest {

    @Autowired
    private BirthStatEntryRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        await().pollInterval(1, SECONDS)
                .atMost(3, SECONDS)
                .until(() -> repository.count() == 0);
    }

    @Test
    void testSaveAndFindById() {
        BirthStatEntry entry = new BirthStatEntry();
        entry.setId(1L);
        entry.setDob(LocalDate.of(2000, 1, 1));
        entry.setTown("Springfield");

        repository.save(entry);

        Optional<BirthStatEntry> found = repository.findById(1L);
        assertThat(found).isPresent();
        assertThat(found.get().getTown()).isEqualTo("Springfield");
        assertThat(found.get().getDob()).isEqualTo(LocalDate.of(2000, 1, 1));
    }

    @Test
    void testUniqueConstraint() {
        BirthStatEntry entry1 = new BirthStatEntry();
        entry1.setId(1L);
        entry1.setDob(LocalDate.of(1990, 5, 20));
        entry1.setTown("Springfield");
        repository.save(entry1);

        BirthStatEntry entry2 = new BirthStatEntry();
        entry2.setId(3L);
        entry2.setDob(LocalDate.of(1990, 5, 20));
        entry2.setTown("Springfield");

        assertThrows(DataIntegrityViolationException.class, () -> repository.saveAndFlush(entry2));
    }

    @Test
    void testSaveWithNullPrimaryKey() {
        BirthStatEntry entry = new BirthStatEntry();
        entry.setId(null);
        entry.setDob(LocalDate.of(1985, 7, 15));
        entry.setTown("Shelbyville");
        assertThrows(JpaSystemException.class, () -> repository.saveAndFlush(entry), "Should not allow null primary key");
    }

    @Test
    void testSaveWithNullDob() {
        BirthStatEntry entry = new BirthStatEntry();
        entry.setId(100L);
        entry.setDob(null);
        entry.setTown("Springfield");
        assertThrows(ConstraintViolationException.class, () -> repository.saveAndFlush(entry), "Should not allow null dob");
    }

    @Test
    void testSaveWithNullTown() {
        BirthStatEntry entry = new BirthStatEntry();
        entry.setId(101L);
        entry.setDob(LocalDate.of(1990, 1, 1));
        entry.setTown(null);
        assertThrows(ConstraintViolationException.class, () -> repository.saveAndFlush(entry), "Should not allow null town");
    }

    @Test
    void testSaveWithFutureDob() {
        BirthStatEntry entry = new BirthStatEntry();
        entry.setId(102L);
        entry.setDob(LocalDate.now().plusDays(1));
        entry.setTown("Springfield");
        assertThrows(ConstraintViolationException.class, () -> repository.saveAndFlush(entry), "Should not allow future dob");
    }

    @Test
    void testSaveWithTownTooLong() {
        BirthStatEntry entry = new BirthStatEntry();
        entry.setId(103L);
        entry.setDob(LocalDate.of(1990, 1, 1));
        entry.setTown("A".repeat(51));
        assertThrows(ConstraintViolationException.class, () -> repository.saveAndFlush(entry), "Should not allow town longer than 50 characters");
    }
}
