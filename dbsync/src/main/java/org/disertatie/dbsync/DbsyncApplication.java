package org.disertatie.dbsync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableKafka
public class DbsyncApplication {

	// @Autowired
	// private MyMongoService mongoService;
	// @Autowired
	// private MySQLService sqlService;
	// private int i = 0;

	public static void main(String[] args) {
		SpringApplication.run(DbsyncApplication.class, args);
	}

	// @Override
	// public void run(String... args) throws Exception {
		// System.out.println("Started...");
		// mongoService.test();
		// sqlService.test();
		//scheduleFixedRateTask();
		// System.out.println("Done...");
	// }

	// @Scheduled(fixedRate = 8000)
	// public void scheduleFixedRateTask() {
	// 	List<String> sqlTables = sqlService.getTables();
	// 	List<String> mongoTables = mongoService.getCollections();

	// 	for (String table : sqlTables) {
	// 		if (!mongoTables.contains(table)) {
	// 			// Schema schema = sqlService.getTableSchema(table);
	// 			mongoService.createCollection(table);
	// 		}
	// 			Data data = sqlService.getTableData(table);
	// 			mongoService.addData(table, data);
	// 	}
	// 	System.out.println("Fixed rate task - " + i++);
	// }
}
