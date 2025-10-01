package foo.kafka.simple.producer.rest;

import foo.avro.birth.BirthEvent;
import foo.kafka.simple.producer.service.BirthMapper;
import foo.kafka.simple.producer.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/births")
@RequiredArgsConstructor
public class ApiController {

    private final KafkaProducerService service;
    private final BirthMapper mapper;

    @PostMapping
    public ResponseEntity<Birth> createUser(@RequestBody Birth input) {
        BirthEvent event = mapper.toBirthEvent(input);
        log.info("Producing event: {}", event);
        service.sendKafka(event);
        return ResponseEntity.ok(input);
    }
}
