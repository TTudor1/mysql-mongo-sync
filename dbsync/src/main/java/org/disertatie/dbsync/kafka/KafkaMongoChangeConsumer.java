package org.disertatie.dbsync.kafka;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.disertatie.dbsync.common.DeserializerProvider;
import org.disertatie.dbsync.common.event.CaputureKafkaEvent;
import org.disertatie.dbsync.common.event.CaputureKafkaEventMongo;
import org.disertatie.dbsync.common.event.Payload;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.disertatie.dbsync.sql.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


@Service
public class KafkaMongoChangeConsumer {

    @Autowired
    MySQLService sqlService;
    @Autowired
    MyMongoService mongoService;
    @Autowired
    DeserializerProvider serde;

    private static final String prefix = "trt2";
    @KafkaListener(topics = prefix + ".test.data_examplesql", groupId = "test-group")
    public void consume(ConsumerRecord<String, String> payload) {

        System.out.println("-------DETECTED MONGO RECORD CHANGE-------");
        byte[] payloadBytes =  payload.value() == null ? null :  payload.value().getBytes();
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  
        CaputureKafkaEventMongo test1 = null;
        Object before = null;
        Object after = null;

        try {
            test1 = mapper.readValue(payloadBytes, CaputureKafkaEventMongo.class);
            if (test1.getPayload().getBefore() != null) {
                before = mapper.readValue(test1.getPayload().getBefore().getBytes(), Object.class);
            }
            if (test1.getPayload().getAfter() != null) {
                after = mapper.readValue(test1.getPayload().getAfter().getBytes(), Object.class);
            }
        } catch (StreamReadException e) {
            e.printStackTrace();
        } catch (DatabindException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (test1 == null) {
            return;
        }
        Map<String, Object> beforeMap = (Map) before;
        Map<String, Object> afterMap = (Map) after;

        // TODO get table schema, get field types, deserialize event, deserialize object, store last processed message

        if(beforeMap != null) {
            Integer id = (Integer) beforeMap.get("_id");
                beforeMap.put("id", id);
                beforeMap.remove("_id");
        }
        if(afterMap != null) {
            Integer id = (Integer)afterMap.get("_id");
            afterMap.put("id", id);
            afterMap.remove("_id");
        }

        if (test1.getPayload().getTs_ms() > mongoService.getLastUpdate("sql")) {
            switch (test1.getPayload().getOp()) {
                case "c": //create
                sqlService.insertRecord("data_examplesql", afterMap);
                // mongoService.kafkaDataInsert("data_examplesql", test);
                    break;
                case "u": //update
                // mongoService.kafkaDataUpdate("data_examplesql", test);
                    break;
                case "d": //delete
                // mongoService.kafkaDataDelete("data_examplesql", test);
                    break;
            }
        }
    }
}
