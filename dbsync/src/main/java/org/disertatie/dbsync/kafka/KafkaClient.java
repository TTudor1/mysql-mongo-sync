package org.disertatie.dbsync.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

import org.springframework.scheduling.annotation.Scheduled;

@SuppressWarnings("all")
// @Service
public class KafkaClient {

    // String filePath = "C:\\Users\\turtu\\Desktop\\a.txt";
    // Properties properties = new Properties();
    // KafkaConsumer kafkaConsumer;

    // public KafkaClient() {
    //     properties.put("bootstrap.servers", "localhost:9092");
    //     properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //     properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //     properties.put("group.id", "test-group");
    //     kafkaConsumer = new KafkaConsumer(properties);
    //     List topics = new ArrayList();
    //     topics.add("dise-tudorTopic.db_example.data_examplesql");
    //     kafkaConsumer.subscribe(topics);
    // }

    // @Scheduled(fixedRate = 8000)
    // public void pollKafka() {
    //     try {
    //         ConsumerRecords records = kafkaConsumer.poll(2000);
    //         records.forEach(xrecord -> {
    //             ConsumerRecord record = (ConsumerRecord) xrecord;
    //             System.out.println("WORKS!");

    //             try {
    //                 BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
    //                 writer.newLine();
    //                 writer.write(record.toString());
    //                 writer.write(record.toString());
    //                 writer.close();
    //             } catch (Throwable e) {
    //                 System.out.println(e.getMessage());
    //             }
    //         });
    //     } catch (Throwable e) {
    //         System.out.println(e.getMessage());
    //         kafkaConsumer.close();
    //     } finally {
    //     }

    //     System.out.println("End KAFKA Poll");
    // }
    
    // @PreDestroy
    // public void destroy() {
    //     System.out.println("Callback triggered - @PreDestroy.");
    //       kafkaConsumer.close();
    // }
}
