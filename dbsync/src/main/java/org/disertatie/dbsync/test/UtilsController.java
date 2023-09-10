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
    mongoTemplate.remove(q, "bilet");
    mongoTemplate.remove(q, "zbor");
    mongoTemplate.remove(q, "client");

    jdbcTemplate.execute("delete from bilet where id != 99999");
    jdbcTemplate.execute("delete from zbor where id != 99999");
    jdbcTemplate.execute("delete from client where id != 99999");
    return "done";
  }
}
