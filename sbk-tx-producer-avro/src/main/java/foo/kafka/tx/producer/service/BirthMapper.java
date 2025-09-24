package foo.kafka.tx.producer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.producer.persistence.Birth;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface BirthMapper {

    BirthEvent toBirthEvent(Birth birth);
}

