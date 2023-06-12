package org.disertatie.dbsync.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CustomConsumer {
    
    boolean noop = false;
    public void start(String topic, KafkaMongoChangeConsumer mongoChangeConsumer) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-test-g1" + new Random().nextInt(5000));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("group.id", "test-group");

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        KafkaNoopConsumer noopConsumer = new KafkaNoopConsumer();

        // Subscribe to the topic(s)
        consumer.subscribe(Collections.singleton(topic));

        // Start consuming messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    if (noop) {
                        noopConsumer.consume(record);
                    } else {
                        mongoChangeConsumer.consume(record);
                    }
                }

                consumer.commitAsync();
            }
        } finally {
            consumer.close();
        }
    }

    public void start(String topic, KafkaSqlChangeConsumer sqlChangeConsumer) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-test-g1" + new Random().nextInt(5000));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("group.id", "test-group");

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        KafkaNoopConsumer noopConsumer = new KafkaNoopConsumer();

        // Subscribe to the topic(s)
        consumer.subscribe(Collections.singleton(topic));

        // Start consuming messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    if (noop) {
                        noopConsumer.consume(record);
                    } else {
                        sqlChangeConsumer.consume(record);
                    }
                }

                consumer.commitAsync();
            }
        } finally {
            consumer.close();
        }
    }
}