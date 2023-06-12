package org.disertatie.dbsync.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
public class KafkaNoopConsumer {
    
    public void consume(ConsumerRecord<String, String> kafkaPayload) {
        System.out.println("message dropped");
    }
}
