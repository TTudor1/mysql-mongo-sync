package org.disertatie.dbsync.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class MongoTestController {

	@Autowired
	MongoTemplate mongoTemplate;
	@Autowired
    private JdbcTemplate jdbcTemplate;

    private int[] tests = new int[] {2000, 1000, 500, 100, 60, 30};
    private int[] finalCount = new int[] {2000, 3000, 3500, 3600, 3660, 3690};
    private int[] finalCountForDelete = new int[] {1690, 690, 190, 90, 30, 0};

  @GetMapping("/testMongoInsertDelay")
  String all() throws InterruptedException {
    for (int i = 1; i <= 10; i++) {
        int randid = 220 + new Random().nextInt(16000);
		Map<String,Object> entity = new HashMap<>();
        List<Map<String, Object>> rows = new ArrayList<>();
		entity.put("_id", randid);
		entity.put("CNP", "12"+randid);
		entity.put("Nume", "Tud");
		mongoTemplate.insert(entity, "Client");
        long start = System.currentTimeMillis();

		String query = "select * from " + "Client where id ='" + randid +"'";
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
  String testMongoBulkInsert() throws InterruptedException {
    List<Map<String, Object>> objects = new ArrayList<>();
    int id = 0;
    for (int i = 0; i < tests.length; i++) {
        objects = new ArrayList<>();
        for (int j = 0; j < tests[i]; j++) {
            id++;
            Map<String, Object> entity = new HashMap<>();
            entity.put("_id", id);
            entity.put("CNP", "193"+id);
            entity.put("Nume", "Tudor");
            objects.add(entity);
        }        
        BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "Client");
        for (Map<String, Object> document : objects) {
            bulkOps.insert(document);
        }
        bulkOps.execute();
        long start = System.currentTimeMillis();
        String query = "select count(*) from " + "Client";
        Integer res = 0;
        while (res != finalCount[i]) {
            Thread.sleep(10);
            res = jdbcTemplate.queryForObject(query, Integer.class);
            if (res == null) {
                res = 0;
            }
        }
        System.out.println("done " + tests[i] + " in " + (System.currentTimeMillis() - start));
    }
    return "done";
  }

  @GetMapping("/testMongoUpdate")
  String testMongoUpdate() throws InterruptedException {
    int id = 0;
    for (int i = 0; i < tests.length; i++) {
        BulkOperations bulkOps = mongoTemplate.bulkOps(BulkMode.UNORDERED, "Client");
        for (int j = 0; j < tests[i]; j++) {
            id++;
            Query query = new Query();
            query.addCriteria(Criteria.where("_id").is(id));

            Update update = new Update();
            update.set("Nume", "Tudor1");
            bulkOps.updateOne(query, update);
        }
        bulkOps.execute();

        long start = System.currentTimeMillis();
        String query = "select count(*) from " + "Client where Nume = 'Tudor1'";
        Integer res = 0;
        while (res != finalCount[i]) {
            Thread.sleep(10);
            res = jdbcTemplate.queryForObject(query, Integer.class);
            if (res == null) {
                res = 0;
            }
        }
        System.out.println("done " + tests[i] + " in " + (System.currentTimeMillis() - start));
    }
    return "done";
  }

  @GetMapping("/testMongoDelete")
  String testMongoDelete() throws InterruptedException {
    int id = 0;
    List<Integer> ids;
    for (int i = 0; i < tests.length; i++) {
        ids = new ArrayList<>();
        for (int j = 1; j <= tests[i]; j++) {
            id++;
            ids.add(id);
        }
        Query query = Query.query(Criteria.where("_id").in(ids));
        mongoTemplate.remove(query, "Client");

        long start = System.currentTimeMillis();

        String queryString = "select count(*) from " + "Client";
        Integer res = -1;
        while (res != finalCountForDelete[i]) {
            Thread.sleep(10);
            res = jdbcTemplate.queryForObject(queryString, Integer.class);
            if (res == null) {
                res = 0;
            }
        }
        System.out.println("done " + tests[i] + " in " + (System.currentTimeMillis() - start));
    }
    return "done";
  }

  static int testId = 3;
  @GetMapping("/testMongoFKInsert")
  String testMongoFKInsert() throws InterruptedException {
    
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


}