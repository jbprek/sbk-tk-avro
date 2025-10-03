package foo.kafka.tx.consumer;

import foo.avro.birth.BirthEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;

import foo.kafka.tx.consumer.service.Processor;
import org.springframework.messaging.Message;

@SpringBootApplication
@Slf4j
public class SbkCloudTxConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SbkCloudTxConsumerApplication.class, args);
    }




}
