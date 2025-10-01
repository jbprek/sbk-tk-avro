package foo.kafka.simple.producer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.simple.producer.rest.Birth;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface BirthMapper {

    BirthEvent toBirthEvent(Birth birth);
}

