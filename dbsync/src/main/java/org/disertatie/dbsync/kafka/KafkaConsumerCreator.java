package org.disertatie.dbsync.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.disertatie.dbsync.sql.MySQLService;
import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Autowired;

@Service
public class KafkaConsumerCreator {
  Properties properties = new Properties();
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
    Map<String, TopicListing> topics;

    try (AdminClient adminClient = AdminClient.create(properties)) {
      ListTopicsResult topicsResult = adminClient.listTopics();
      KafkaFuture<java.util.Map<String, TopicListing>> topicsFuture = topicsResult.namesToListings();
      topics = topicsFuture.get();
        
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      return;
    }
    StringBuilder sb = new StringBuilder();
    String tblOrColName = "";
    for (String topic : topics.keySet()) {
      final KafkaChangeConsumer consumer;
      if (topic.startsWith(sqlTopicPrefix + "." + sqlDbName) && isNotExcluded(topic)) {
        tblOrColName = topic.replaceFirst(sqlTopicPrefix, "")
          .replaceFirst(sqlDbName, "").replace(".", "");
        sb.append("found sql table: " + tblOrColName).append("\n");
        consumer = new KafkaSqlChangeConsumer(tblOrColName, sqlService, mongoService);
      } else if(topic.startsWith(mongoTopicPrefix + "." + mongoDbName) && isNotExcluded(topic)) {
        tblOrColName = topic.replaceFirst(mongoTopicPrefix, "")
          .replaceFirst(mongoDbName, "").replace(".", "");
        sb.append("found mongo collection: " + tblOrColName).append("\n");
        consumer = new KafkaMongoChangeConsumer(tblOrColName, sqlService, mongoService);
      } else {
        consumer = null;
      }
      if (consumer == null) {
        continue;
      }
      CustomConsumer c = new CustomConsumer();
      Thread thread = new Thread(() -> {
        c.start(topic, consumer);
      });
      thread.start();
    }
    System.out.println("List of topics:");
    System.out.println("---------------------------------------------------");
    System.out.println(sb.toString());
    System.out.println("---------------------------------------------------");
  }

  private boolean isNotExcluded(String topic) {
    return !topic.contains("_seq")  && !topic.contains("updateStats");
  }
}
