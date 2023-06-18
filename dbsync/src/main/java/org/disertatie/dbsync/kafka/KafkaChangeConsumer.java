package org.disertatie.dbsync.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaChangeConsumer {

    public void consume(ConsumerRecord<String, String> kafkaPayload) throws InterruptedException;
    
}
