package org.disertatie.dbsync.nosql;

import org.disertatie.dbsync.nosql.model.DataExample;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DataExampleRepository extends MongoRepository<DataExample, String> {
    
    
}
