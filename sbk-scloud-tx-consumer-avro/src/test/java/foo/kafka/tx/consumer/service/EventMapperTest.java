package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EventMapperTest {
    @Test
    void testBirthEventToBirthentity() {
        EventMapper mapper = new EventMapperImpl();
        BirthEvent birthEvent =  BirthEvent.newBuilder()
                .setId(1L)
                .setName("John Doe")
                .setDob( LocalDate.of(1990, 1, 1))
                .setTown("Springfield")
                .setWeight(new BigDecimal(2.5))
                .build();
                // new BirthEvent(1L, ,, "Springfield");

        var entity = mapper.eventToEntity(birthEvent);

        assertNotNull(entity);
        assertEquals(birthEvent.getId(), entity.getId());
        assertEquals(birthEvent.getDob(), entity.getDob());
        assertEquals(birthEvent.getTown(), entity.getTown());
    }
}