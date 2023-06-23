package org.disertatie.dbsync.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UtilsController {
    
    
	@Autowired
	MongoTemplate mongoTemplate;
	@Autowired
    private JdbcTemplate jdbcTemplate;

      @GetMapping("/reset")
  String all() throws InterruptedException {
    Query q = new Query();
    mongoTemplate.remove(q, "Bilet");
    mongoTemplate.remove(q, "Zbor");
    mongoTemplate.remove(q, "Client");

    jdbcTemplate.execute("delete from bilet where id != 9999");
    jdbcTemplate.execute("delete from zbor where id != 9999");
    jdbcTemplate.execute("delete from client where id != 9999");
    return "done";
  }
}



/*

@GetMapping("/testCreateDelete")
//test create + delete
String testCreateDelete() throws InterruptedException {
  List<Map<String, Object>> objects = new ArrayList<>();
  for (int j = 1; j <= 900; j++) {
      Map<String, Object> entity = new HashMap<>();
      entity.put("_id", j);
  entity.put("CNP", "12"+j);
  entity.put("Nume", "Tud");
      objects.add(entity);
  }
  long start = System.currentTimeMillis();
  
  BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "Client");
  
  for (Map<String, Object> document : objects) {
      bulkOps.insert(document);
  }
  bulkOps.execute();
  MongoCollection<Document> collection = mongoTemplate.getCollection("Client");
  // Define the range of IDs
  int startId = 1;
  int endId = 900;

  // Build the query
  Document query = new Document("_id", new Document("$gte", startId)
          .append("$lte",endId));

  // Delete the documents in the specified range
  Thread.sleep(1000);
  collection.deleteMany(query);

  // String query2 = "select count(*) from " + "client";
  // int res = 0;
  // while (res != 2000) {//
  //     // System.out.println(res);
  //     Thread.sleep(100);
  //     res = jdbcTemplate.queryForObject(query2, Integer.class);
  // }
  System.out.println("done in " + (System.currentTimeMillis() - start));
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
  entity.put("CNP", "12"+j);
  entity.put("Nume", "Tud");
      objects.add(entity);
  }
  long start = System.currentTimeMillis();
  
  BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "Client");
  
  for (Map<String, Object> document : objects) {
      bulkOps.insert(document);
  }
  bulkOps.execute();
  MongoCollection<Document> collection = mongoTemplate.getCollection("Client");
  // Define the range of IDs
  int startId = 1;
  int endId = 900;

  // Build the query
  Document query = new Document("_id", new Document("$gte", startId)
          .append("$lte",endId));

  // Delete the documents in the specified range
  Thread.sleep(1000);
  collection.deleteMany(query);

  // String query2 = "select count(*) from " + "client";
  // int res = 0;
  // while (res != 2000) {//
  //     // System.out.println(res);
  //     Thread.sleep(100);
  //     res = jdbcTemplate.queryForObject(query2, Integer.class);
  // }
  System.out.println("done in " + (System.currentTimeMillis() - start));
  // Thread.sleep(400);

  return "done";
}
 */