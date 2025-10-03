package foo.kafka.tx.consumer.persistence;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
// Needed for proxying tests
public class BirthStatDao {

    private final BirthStatEntryRepository repository;

    public BirthStatEntry saveAndFlush(BirthStatEntry entry) {
        return repository.saveAndFlush(entry);
    }
}
