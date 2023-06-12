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

    Properties properties = new Properties();
    KafkaConsumer kafkaConsumer;
    String sqlTopicPrefix = "trt";
    String sqlDbName = "db_example";
    String mongoTopicPrefix = "trt2";
    String mongoDbName = "test";

    @Autowired
    public KafkaConsumerCreator(MySQLService sqlService, MyMongoService mongoService) {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-test-g2");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("group.id", "test-group");

        try (AdminClient adminClient = AdminClient.create(properties)) {
            // Get the list of topics
            ListTopicsResult topicsResult = adminClient.listTopics();
            KafkaFuture<java.util.Map<String, TopicListing>> topicsFuture = topicsResult.namesToListings();

            // Wait for the topics result
            java.util.Map<String, TopicListing> topics = topicsFuture.get();

            // Print the list of topics
            StringBuilder sb = new StringBuilder();
            String tblOrColName = "";
            for (String topic : topics.keySet()) {
                if (topic.startsWith(sqlTopicPrefix)) { 
                    if (topic.startsWith(sqlTopicPrefix + "." + sqlDbName) && isNotExcluded(topic)) {
                        tblOrColName = topic.replace(sqlTopicPrefix, "").replace(sqlDbName, "").replace(".", "");
                       sb.append("found sql table: " + tblOrColName).append("\n");
                        KafkaSqlChangeConsumer sqlChangeConsumer = new KafkaSqlChangeConsumer(tblOrColName, sqlService, mongoService);
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
                if (topic.startsWith(mongoTopicPrefix)) {
                    if(topic.startsWith(mongoTopicPrefix + "." + mongoDbName) && isNotExcluded(topic)) {
                        tblOrColName = topic.replace(mongoTopicPrefix, "").replace(mongoDbName, "").replace(".", "");
                        sb.append("found mongo collection: " + tblOrColName).append("\n");
                        KafkaMongoChangeConsumer mongoChangeConsumer = new KafkaMongoChangeConsumer(tblOrColName, sqlService, mongoService);
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
            System.out.println("List of topics:");
            System.out.println("---------------------------------------------------");
            System.out.println(sb.toString());
            System.out.println("---------------------------------------------------");

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    private boolean isNotExcluded(String topic) {
        return !topic.contains("_seq")  && !topic.contains("updateStats");
    }
}
