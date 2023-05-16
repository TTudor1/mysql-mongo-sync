package org.disertatie.dbsync.nosql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.disertatie.dbsync.common.CaputureKafkaEvent;
import org.disertatie.dbsync.common.Data;
import org.disertatie.dbsync.nosql.model.DataExample;
import org.disertatie.dbsync.sql.DataExampleSQL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoIterable;

@Service
public class MyMongoService  {
    
    private DataExampleRepository repository;
    MongoTemplate mongoTemplate;

    @Autowired
    public MyMongoService(DataExampleRepository repository) {
        this.repository = repository;
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

    public void kafkaDataInsert(String schema, CaputureKafkaEvent data) {
        mongoTemplate.insert(data.getAfter(), data.getSource().getTable());
    }
    public void kafkaDataUpdate(String schema, CaputureKafkaEvent data) {
        mongoTemplate.save(data.getAfter(), data.getSource().getTable());
    }
    public void kafkaDataDelete(String schema, CaputureKafkaEvent data) {
        mongoTemplate.remove(data.getBefore(), data.getSource().getTable());
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
