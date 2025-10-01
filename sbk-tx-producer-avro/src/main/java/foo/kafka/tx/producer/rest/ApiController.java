package foo.kafka.tx.producer.rest;

import foo.kafka.tx.producer.persistence.Birth;
import foo.kafka.tx.producer.persistence.BirthRepository;
import foo.kafka.tx.producer.service.ProcessorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/births")
@RequiredArgsConstructor
public class ApiController {

    private final ProcessorService service;
    private final BirthRepository repository;

    @PostMapping
    public ResponseEntity<Birth> createUser(@RequestBody Birth input) {
        Birth saved = service.sendAndStore(input);
        return ResponseEntity.ok(saved);
    }

    @GetMapping
    public ResponseEntity<List<Birth>> getAllUsers() {
        var userEntities = repository.findAll();
        var allEntries = userEntities.stream()
                .toList();
        return ResponseEntity.ok(allEntries);
    }
}
