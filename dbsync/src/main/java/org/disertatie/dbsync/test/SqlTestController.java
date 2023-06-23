package org.disertatie.dbsync.test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SqlTestController {
    
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private int[] tests = new int[] {2000, 1000, 500, 100, 60, 30};
    private int[] finalCount = new int[] {2000, 3000, 3500, 3600, 3660, 3690};
    private int[] finalCountForDelete = new int[] {1690, 690, 190, 90, 30, 0};

  @GetMapping("/testSqlInsertDelay")
  String testSqlInsertDelay() throws InterruptedException {
    for (int i = 1; i <= 10; i++) {
        int randid = 220 + new Random().nextInt(16000);
		String query = "insert into client VALUES ("+ i +",'" + randid +"', '"+ randid + "')";
        // long start = System.currentTimeMillis();
        jdbcTemplate.execute(query);
        long start = System.currentTimeMillis();

		Query q = new Query();

		long count = mongoTemplate.count(q, "Client");
        while (count != i) {
            count = mongoTemplate.count(q, "Client");
            Thread.sleep(15);
        }
        System.out.println("done in " + (System.currentTimeMillis() - start));
		Thread.sleep(400);

    }
    return "done";
  }
  
  @GetMapping("/testSqlBulkInsert")
  String testSqlBulkInsert() throws InterruptedException {
    List<Object[]> objects = new ArrayList<>();
    int id = 0;
    for (int i = 0; i < tests.length; i++) {
        objects = new ArrayList<>();
        for (int j = 0; j < tests[i]; j++) {
            id++;
            Object[] o = new Object[3];
            o[0] = id;
            o[1] = "193" + id;
            o[2] = "Tudor";
            objects.add(o);
        }
        String sql = "INSERT INTO client (id, CNP, Nume) VALUES (?, ?, ?)";
        jdbcTemplate.batchUpdate(sql, objects);
    
        long start = System.currentTimeMillis();
        Query q = new Query();

        long count = mongoTemplate.count(q, "Client");
        while (count != finalCount[i]) {
            // System.out.println(count);
            Thread.sleep(30);
            count = mongoTemplate.count(q, "Client");
        }
        System.out.println("done " + tests[i] + " in " + (System.currentTimeMillis() - start));
    }
    return "done";
  }

  @GetMapping("/testSqlUpdate")
  String testSqlUpdate() throws InterruptedException {
    for (int j = 0; j < tests.length; j++) {
        int[] wj = new int[]{j};
        String sql = "UPDATE client SET Nume = ? WHERE id = ?";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                int myId;
                if (wj[0] == 0) {
                    myId = i + 1;
                } else {
                    myId = finalCount[wj[0] - 1] + i + 1;
                }
                preparedStatement.setString(1, "Tudor3");
                preparedStatement.setInt(2, myId);
            }

            @Override
            public int getBatchSize() {
                return tests[wj[0]];
            }
        });
    
        long start = System.currentTimeMillis();
        Query q = new Query(Criteria.where("Nume").is("Tudor3"));

        long count = mongoTemplate.count(q, "Client");
        while (count != finalCount[j]) {
            Thread.sleep(15);
            count = mongoTemplate.count(q, "Client");
        }
        System.out.println("done " + tests[j] + " in " + (System.currentTimeMillis() - start));
    }
    
    return "done";
  }

  @GetMapping("/testSqlDelete")
  String testSqlDelete() throws InterruptedException {
    for (int j = 0; j < tests.length; j++) {
        String sql = "DELETE FROM client WHERE id = ?";
        int[] wj = new int[]{j};
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                int myId;
                if (wj[0] == 0) {
                    myId = i + 1;
                } else {
                    myId = finalCount[wj[0] - 1] + i + 1;
                }
                preparedStatement.setInt(1, myId);
            }

            @Override
            public int getBatchSize() {
                return tests[wj[0]];
            }
        });
    
        long start = System.currentTimeMillis();
        Query q = new Query();

        long count = mongoTemplate.count(q, "Client");
        while (count != finalCountForDelete[j]) {
            Thread.sleep(15);
            count = mongoTemplate.count(q, "Client");
        }
        System.out.println("done " + tests[j] + " in " + (System.currentTimeMillis() - start));
    }
    return "done";
  }
}
