package org.disertatie.dbsync;

// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;

// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.data.mongodb.core.MongoTemplate;
// import org.springframework.jdbc.core.JdbcTemplate;
// import org.springframework.test.context.ContextConfiguration;
// import org.springframework.test.context.junit.jupiter.SpringExtension;
// import org.springframework.test.context.web.WebAppConfiguration;
// import org.springframework.util.Assert;

// @ExtendWith(SpringExtension.class)
// @WebAppConfiguration
class DbsyncApplicationTests {
	
	// @Autowired
	// MongoTemplate mongoTemplate;
	// @Autowired
    // private JdbcTemplate jdbcTemplate;

	// @Test
	// void testwriteDBs() throws Exception {
	// 	Map<String,Object> entity = new HashMap<>();
	// 	entity.put("_id", 1233);
	// 	mongoTemplate.insert(entity, "data_examplesql");

	// 	Thread.sleep(500);

	// 	String query = "select * from " + "data_examplesql";
    //     List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);
	// 	Assert.isTrue(rows.size() == 1, "too few/many records");

	// }

}
