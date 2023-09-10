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
  public void start(String topic, KafkaChangeConsumer changeConsumer) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-test-g1" + new Random().nextInt(5000));
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put("group.id", "test-group");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton(topic));

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
          try {
            changeConsumer.consume(record);
          } catch (InterruptedException e) {
            System.out.println("Thread interrupted");
          }
        }
        consumer.commitAsync();
      }
    } finally {
      consumer.close();
    }
  }
}