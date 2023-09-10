package org.disertatie.dbsync.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;

@RestController
class MongoTestController {
  FakeValuesService fakeValuesService = new FakeValuesService(new Locale("en-GB"), new RandomService());
  Faker faker = new Faker();

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
    mongoTemplate.insert(client, "Client");

    Map<String, Object> zbor = new HashMap<>();
    zbor.put("_id", testId);
    zbor.put("nume", "Asia-Europa");
    mongoTemplate.insert(zbor, "Zbor");

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


  @GetMapping("/testMongoFKMultiInsert")
  String testMongoFKMultiInsert() throws InterruptedException {
    System.out.println("Start");
    for (int j = 2; j < 3; j++) {
        List<Map<String, Object>> clienti = new ArrayList<>();
        List<Map<String, Object>> zboruri = new ArrayList<>();
        List<Map<String, Object>> bilete = new ArrayList<>();
        while (true) {
          String query = "select count(*) from " + "client";
          Integer res1 = jdbcTemplate.queryForObject(query, Integer.class);
          query = "select count(*) from " + "zbor";
          Integer res2 = jdbcTemplate.queryForObject(query, Integer.class);
          query = "select count(*) from " + "bilet";
          Integer res3 = jdbcTemplate.queryForObject(query, Integer.class);
          // System.out.println(res1 + " " + res2 + " " + res3 + "  " + System.currentTimeMillis());
          if (res1 + res2 + res3 != 0) {
            System.out.println("Still deleting");
            delAll();
            Thread.sleep(3000);
          } else {
            System.out.println("Deleted");
            break;
          }
        }
        for (int i = 1; i <= tests[j]; i++) {
          // int randid = 220 + new Random().nextInt(16000);
          Map<String, Object> client = new HashMap<>();
          client.put("_id", i);
          client.put("CNP", "1913213213213"+i);
          client.put("Nume", "Tudor");
          clienti.add(client);

          Map<String, Object> zbor = new HashMap<>();
          zbor.put("_id", i);
          zbor.put("nume", "Asia-Europa");
          zboruri.add(zbor);

          Map<String, Object> bilet = new HashMap<>();
          bilet.put("_id", i);
          bilet.put("id_zbor", i);
          bilet.put("id_client", i);
          bilete.add(bilet);
        }
      BulkOperations bulkOps1 = mongoTemplate.bulkOps(BulkMode.UNORDERED, "Client");
      for (Map<String, Object> document : clienti) {
          bulkOps1.insert(document);
      }

      BulkOperations bulkOps2 = mongoTemplate.bulkOps(BulkMode.UNORDERED, "Zbor");
      for (Map<String, Object> document : zboruri) {
          bulkOps2.insert(document);
      }

      BulkOperations bulkOps3 = mongoTemplate.bulkOps(BulkMode.UNORDERED, "bilet");
      for (Map<String, Object> document : bilete) {
          bulkOps3.insert(document);
      }

      try {
        bulkOps1.execute();
        bulkOps2.execute();
        bulkOps3.execute();
      } catch (Exception e) {
        System.out.println("Oh no");
      }

      long start = System.currentTimeMillis();
      while (true) {
        String query = "select count(*) from " + "client";
        Integer res1 = jdbcTemplate.queryForObject(query, Integer.class);
        query = "select count(*) from " + "zbor";
        Integer res2 = jdbcTemplate.queryForObject(query, Integer.class);
        query = "select count(*) from " + "bilet";
        Integer res3 = jdbcTemplate.queryForObject(query, Integer.class);
        // System.out.println(res1 + " " + res2 + " " + res3 + "  " + System.currentTimeMillis());
        if (res1 + res2 + res3 == 3*tests[j]) {
          System.out.println("done in " + (System.currentTimeMillis() - start) + " for " + tests[j]);
          break;
        }
        Thread.sleep(300);
      }
    }
    System.out.println("done");
    testId++;
    return "done";
  }


  private void delAll() throws InterruptedException {
    Query q = new Query();
    mongoTemplate.remove(q, "bilet");
    mongoTemplate.remove(q, "Zbor");
    mongoTemplate.remove(q, "Client");

    jdbcTemplate.execute("delete from bilet where id != 9999");
    jdbcTemplate.execute("delete from zbor where id != 9999");
    jdbcTemplate.execute("delete from client where id != 9999");
  }

}