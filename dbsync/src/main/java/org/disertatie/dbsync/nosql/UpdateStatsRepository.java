package org.disertatie.dbsync.nosql;

import org.disertatie.dbsync.nosql.model.UpdateStats;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface UpdateStatsRepository extends MongoRepository<UpdateStats, String> {
    
    
}
