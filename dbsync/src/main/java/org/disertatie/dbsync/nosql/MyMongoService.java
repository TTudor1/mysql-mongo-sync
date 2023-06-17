package org.disertatie.dbsync.nosql;

import java.util.List;

import org.disertatie.dbsync.kafka.model.Payload;
import org.disertatie.dbsync.nosql.model.UpdateStats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
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

    public void kafkaDataInsert(String schema, Payload data) {
        // System.out.println("CREATING in mongo" + data.getAfter().get("_id"));
        Object result = mongoTemplate.findById(data.getAfter().get("_id"), Object.class, schema);
        if (result == null) {
            try {
            mongoTemplate.insert(data.getAfter(), data.getSource().getTable());
            } catch (DuplicateKeyException e) {
                System.out.println("Duplicate key exception for id " + data.getAfter().get("_id") + " for collection " + schema);
            }
        }
    }
    
    public void kafkaDataUpdate(String schema, Payload data) {
        mongoTemplate.save(data.getAfter(), data.getSource().getTable());
    }

    public void kafkaDataDelete(String schema, Payload data) {
        // System.out.println("DELETING in mongo" + data.getBefore().get("_id"));
        Object result = mongoTemplate.findById(data.getBefore().get("_id"), Object.class, schema);
        if (result != null) {
            mongoTemplate.remove(data.getBefore(), data.getSource().getTable());
        }
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

    public boolean existsWithId(int id, String schema) {
        Object result = mongoTemplate.findById(id, Object.class, schema);
        return result != null;
    }   
}
