package foo.kafka.tx.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaHelper {

    public static String getRecordInfo(ConsumerRecord<?, ?> record) {
        return "topic %s:(%s,%s)"
                .formatted(record.topic(), record.partition(), record.offset());
    }
}
