package org.disertatie.dbsync.nosql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.disertatie.dbsync.common.Data;
import org.disertatie.dbsync.common.event.Payload;
import org.disertatie.dbsync.nosql.model.UpdateStats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@Service
public class MyMongoService  {
    
    private UpdateStatsRepository statsRepository;
    MongoTemplate mongoTemplate;

    @Autowired
    public MyMongoService(UpdateStatsRepository statsRepository) {
        this.statsRepository = statsRepository;
        MongoClient mongo = MongoClients.create();
        MongoTemplate mongoTemplate = new MongoTemplate(mongo, "test");
        this.mongoTemplate = mongoTemplate;
    }

    public List<String> getCollections() {
        return new ArrayList<>(mongoTemplate.getCollectionNames());
    }

    public void createCollection(String schema) {
        mongoTemplate.createCollection(schema);
    }

    public void addData(String schema, Data data) {
        for (Map<String, Object> row : data.getRows()) {
            mongoTemplate.insert(row, schema);
        }

    }

    public void kafkaDataInsert(String schema, Payload data) {
        mongoTemplate.insert(data.getAfter(), data.getSource().getTable());
    }
    public void kafkaDataUpdate(String schema, Payload data) {
        mongoTemplate.save(data.getAfter(), data.getSource().getTable());
    }
    public void kafkaDataDelete(String schema, Payload data) {
        mongoTemplate.remove(data.getBefore(), data.getSource().getTable());
    }

    public long getLastUpdate(String db) {
        List<UpdateStats> allStats = statsRepository.findAll();
        if (allStats.size() == 0) {
            return 0;
        }
        UpdateStats mongoStats = allStats.stream().filter(s -> s.getDbName().equals(db)).findAny().orElse(null);
        if (mongoStats == null) {
            return 0;
        } else {
            return mongoStats.getLast();
        }
    }

    public void setLastUpdate(long updateTime, String db) {
        List<UpdateStats> allStats = statsRepository.findAll();
        UpdateStats mongoStats = allStats.stream().filter(s -> s.getDbName().equals(db)).findAny().orElse(null);
        if (mongoStats == null) {
            mongoStats = new UpdateStats();
            mongoStats.setDbName(db);
            mongoStats.setLast(updateTime);
        } else {
            mongoStats.setLast(updateTime);
        }
        statsRepository.save(mongoStats);
    }

    // public void test() {
    //     DataExample ex = new DataExample(null, "asd", 123);
    //     // repository.insert(ex);
        
    //     String map = "function() { for (var key in this) { emit(key, null); } }";
    //     String reduce = "function(key, values) { return null; }";

    //     MongoIterable<String> collections = mongoTemplate.getDb().listCollectionNames();
    //     for (String s : collections) {
    //         System.out.println(s);
    //         MapReduceResults<Document> out = mongoTemplate.mapReduce(s, map,reduce, Document.class);        
    //         for (Document result : out) {
    //             ;
    //             System.out.println(result.get("_id"));
    //         }
    //     }
    // }
    
}
