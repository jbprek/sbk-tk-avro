package foo.kafka.tx.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaHelper {

    public static String getRecordInfo(ConsumerRecord<?, ?> consumerRecord) {
        return "topic %s:(%s,%s)"
                .formatted(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
    }
}
