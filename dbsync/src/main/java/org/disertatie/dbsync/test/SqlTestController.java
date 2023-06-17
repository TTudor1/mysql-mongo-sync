package org.disertatie.dbsync.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.MongoCollection;

@RestController
public class SqlTestController {
    
    
	@Autowired
	private MongoTemplate mongoTemplate;
	@Autowired
    private JdbcTemplate jdbcTemplate;


  @GetMapping("/testSqlInsertDelay")
  String all() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
        int randid = 220 + new Random().nextInt(6000);
		String query = "insert into client id ='" + randid +"' + CNP = '"+ randid + "'";
        long start = System.currentTimeMillis();
        jdbcTemplate.execute(query);

		Query q = new Query();

		long count = mongoTemplate.count(q, "client");
        while (count == 0) {
            count = mongoTemplate.count(q, "client");
            Thread.sleep(5);
        }
        System.out.println("done in " + (System.currentTimeMillis() - start));
		Thread.sleep(400);

    }
    return "done";
  }

  @GetMapping("/testSqlBulkInsert")
  String all2() throws InterruptedException {
    List<Map<String, Object>> objects = new ArrayList<>();
    for (int j = 0; j < 2000; j++) {
        Map<String, Object> entity = new HashMap<>();
        entity.put("_id", j);
        entity.put("quantity", 12);
        entity.put("name", "Tud");
        objects.add(entity);
    }
    long start = System.currentTimeMillis();
    
    BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "data_examplesql");
    
    for (Map<String, Object> document : objects) {
        bulkOps.insert(document);
    }
    
    bulkOps.execute();
    String query = "select count(*) from " + "data_examplesql";
    Integer res = 0;
    while (res != 2000) {//
        // System.out.println(res);
        Thread.sleep(100);
        res = jdbcTemplate.queryForObject(query, Integer.class);
        if (res == null) {
            res = 0;
        }
    }
    System.out.println("done done in " + (System.currentTimeMillis() - start));
    // Thread.sleep(400);

    return "done";
  }

  @GetMapping("/testSqlDelete")
  //delete
  String all3() throws InterruptedException {
    List<Map<String, Object>> objects = new ArrayList<>();
    for (int j = 0; j < 2000; j++) {
        Map<String, Object> entity = new HashMap<>();
        entity.put("_id", j);
        entity.put("quantity", 12);
        entity.put("name", "Tud");
        objects.add(entity);
    }
    long start = System.currentTimeMillis();
    
    BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "data_examplesql");
    
    MongoCollection<Document> collection = mongoTemplate.getCollection("yourCollectionName");

    // Define the range of IDs
    int startId = 1;
    int endId = 100;

    // Build the query
    Document query = new Document("_id", new Document("$gte", new ObjectId(Integer.toHexString(startId)))
            .append("$lte", new ObjectId(Integer.toHexString(endId))));

    // Delete the documents in the specified range
    collection.deleteMany(query);

    
    bulkOps.execute();
    String query2 = "select count(*) from " + "data_examplesql";
    Integer res = 0;
    while (res != 2000) {//
        // System.out.println(res);
        Thread.sleep(100);
        res = jdbcTemplate.queryForObject(query2, Integer.class);
        if (res == null) {
            res = 0;
        }
    }
    System.out.println("done done in " + (System.currentTimeMillis() - start));
    // Thread.sleep(400);

    return "done";
  }

  
  @GetMapping("/testSqlUpdate")
  String all4() throws InterruptedException {
    //aici trebe update
    List<Map<String, Object>> objects = new ArrayList<>();
    for (int j = 0; j < 2000; j++) {
        Map<String, Object> entity = new HashMap<>();
        entity.put("_id", j);
        entity.put("quantity", 12);
        entity.put("name", "Tud");
        objects.add(entity);
    }
    long start = System.currentTimeMillis();
    
    BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "data_examplesql");
    
    for (Map<String, Object> document : objects) {
        bulkOps.insert(document);

    }
    
    bulkOps.execute();
    String query = "select count(*) from " + "data_examplesql";
    Integer res = 0;
    while (res != 2000) {//
        // System.out.println(res);
        Thread.sleep(100);
        res = jdbcTemplate.queryForObject(query, Integer.class);
        if (res == null) {
            res = 0;
        }
    }
    System.out.println("done done in " + (System.currentTimeMillis() - start));
    // Thread.sleep(400);

    return "done";
  }

  @GetMapping("/testSqlCreateDelete")
  //test create + delete
  String all5() throws InterruptedException {
    List<Map<String, Object>> objects = new ArrayList<>();
    for (int j = 1; j <= 900; j++) {
        Map<String, Object> entity = new HashMap<>();
        entity.put("_id", j);
        entity.put("quantity", 12);
        entity.put("name", "Tud");
        objects.add(entity);
    }
    long start = System.currentTimeMillis();
    
    BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "data_examplesql");
    
    for (Map<String, Object> document : objects) {
        bulkOps.insert(document);
    }
    bulkOps.execute();
    MongoCollection<Document> collection = mongoTemplate.getCollection("data_examplesql");
    // Define the range of IDs
    int startId = 1;
    int endId = 900;

    // Build the query
    Document query = new Document("_id", new Document("$gte", startId)
            .append("$lte",endId));

    // Delete the documents in the specified range
    Thread.sleep(1000);
    collection.deleteMany(query);

    // String query2 = "select count(*) from " + "data_examplesql";
    // int res = 0;
    // while (res != 2000) {//
    //     // System.out.println(res);
    //     Thread.sleep(100);
    //     res = jdbcTemplate.queryForObject(query2, Integer.class);
    // }
    System.out.println("done done in " + (System.currentTimeMillis() - start));
    // Thread.sleep(400);

    return "done";
  }

}
