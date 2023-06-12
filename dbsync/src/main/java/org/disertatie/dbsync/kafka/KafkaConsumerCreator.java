package org.disertatie.dbsync.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.disertatie.dbsync.sql.MySQLService;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

@SuppressWarnings("all")
@Service
public class KafkaConsumerCreator {

    String filePath = "C:\\Users\\turtu\\Desktop\\a.txt";
    Properties properties = new Properties();
    KafkaConsumer kafkaConsumer;

    @Autowired
    public KafkaConsumerCreator(MySQLService sqlService, MyMongoService mongoService) {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-test-g2");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("group.id", "test-group");

        // Create the AdminClient instance
        try (AdminClient adminClient = AdminClient.create(properties)) {
            // Get the list of topics
            ListTopicsResult topicsResult = adminClient.listTopics();
            KafkaFuture<java.util.Map<String, TopicListing>> topicsFuture = topicsResult.namesToListings();

            // Wait for the topics result
            java.util.Map<String, TopicListing> topics = topicsFuture.get();

            // Print the list of topics
            System.out.println("---------------------------------------------------");
            System.out.println("List of topics:");
            for (String topic : topics.keySet()) {
                if (topic.startsWith("trt.")) { // data_
                    if (topic.startsWith("trt.db_example.data_examplesql") && !topic.contains("_seq")) {
                        System.out.println("found sql: " + topic);
                        KafkaSqlChangeConsumer sqlChangeConsumer = new KafkaSqlChangeConsumer(sqlService, mongoService);
                        CustomConsumer c = new CustomConsumer();
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                c.start(topic, sqlChangeConsumer);
                            }
                        });
                        thread.start();
                    }
                }
                if (topic.startsWith("trt2.")) {
                    if(topic.startsWith("trt2.test.data") && !topic.contains("_seq")) {
                        System.out.println("found mongo: " + topic);
                        KafkaMongoChangeConsumer mongoChangeConsumer = new KafkaMongoChangeConsumer(sqlService, mongoService);
                        CustomConsumer c = new CustomConsumer();
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                c.start(topic, mongoChangeConsumer);
                            }
                        });
                        thread.start();
                    }
                }
            }
            System.out.println("---------------------------------------------------");

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

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
