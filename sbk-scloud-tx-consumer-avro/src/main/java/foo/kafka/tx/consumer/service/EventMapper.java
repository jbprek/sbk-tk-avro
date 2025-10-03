package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatEntry;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface EventMapper {
    BirthStatEntry eventToEntity(BirthEvent event);
}
