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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.MongoCollection;

@RestController
class MongoTestController {

	@Autowired
	MongoTemplate mongoTemplate;
	@Autowired
    private JdbcTemplate jdbcTemplate;

  @GetMapping("/testMongoInsertDelay")
  String all() throws InterruptedException {
    for (int i = 0; i < 1; i++) {
        int randid = 220 + new Random().nextInt(6000);
		Map<String,Object> entity = new HashMap<>();
        List<Map<String, Object>> rows = new ArrayList<>();
		entity.put("_id", randid);
		entity.put("quantity", 12);
		entity.put("name", "Tud");
        long start = System.currentTimeMillis();
		mongoTemplate.insert(entity, "data_examplesql");

		String query = "select * from " + "data_examplesql where id ='" + randid +"'";
        while (rows.size() == 0) {
            rows = jdbcTemplate.queryForList(query);
            Thread.sleep(5);
        }
        System.out.println("done in " + (System.currentTimeMillis() - start));
		Thread.sleep(400);

    }
    return "done";
  }

  @GetMapping("/testMongoBulkInsert")
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

  static int testId = 3;
  @GetMapping("/testMongoFKInsert")
  String testMongoLargeVolumeInsert() throws InterruptedException {
    
    Map<String, Object> client = new HashMap<>();
    client.put("_id", testId);
    client.put("CNP", "1913213213213"+testId);
    client.put("Nume", "Tudor");
    mongoTemplate.insert(client, "client");

    Map<String, Object> zbor = new HashMap<>();
    zbor.put("_id", testId);
    zbor.put("nume", "Asia-Europa");
    mongoTemplate.insert(zbor, "zbor");

    Map<String, Object> bilet = new HashMap<>();
    bilet.put("_id", testId);
    bilet.put("id_zbor", testId);
    bilet.put("id_client", testId);
    mongoTemplate.insert(bilet, "bilet");
    
    String query = "select count(*) from " + "bilet";
    Thread.sleep(400);
    Integer res = jdbcTemplate.queryForObject(query, Integer.class);

    System.out.println("done, res=" + res);
    testId++;
    return "done";
  }

  @GetMapping("/testMongoDelete")
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

  
  @GetMapping("/testMongoUpdate")
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

  @GetMapping("/testCreateDelete")
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