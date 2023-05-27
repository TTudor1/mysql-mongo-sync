package org.disertatie.dbsync.kafka;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.disertatie.dbsync.common.DeserializerProvider;
import org.disertatie.dbsync.common.event.CaputureKafkaEvent;
import org.disertatie.dbsync.common.event.Payload;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class KafkaSqlChangeConsumer {

    @Autowired
    MyMongoService mongoService;
    @Autowired
    DeserializerProvider serde;

    private static final String prefix = "trt";
    // @KafkaListener(topics = prefix + ".db_example.data_examplesql", groupId = "test-group")
    public void consume(ConsumerRecord<String, String> payload) {

        System.out.println("--------DETECTED SQL RECORD CHANGE--------");
        byte[] payloadBytes =  payload.value() == null ? null :  payload.value().getBytes();
        ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  
        CaputureKafkaEvent test1 = null;
        try {
            test1 = mapper.readValue(payloadBytes, CaputureKafkaEvent.class);
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
        Payload test = test1.getPayload();
        // Schema schema = test1.getSchema();
        if (test != null) {
            System.out.println(test.getTs_ms() + " " + test.getOp());
        } else {
            System.out.println("null object");
            return;
        }
        if(test.getBefore() != null) {
            Integer id = (Integer) test.getBefore().get("id");
                test.getBefore().put("_id", id);
                test.getBefore().remove("id");
        }
        if(test.getAfter() != null) {
            Integer id = (Integer)((Map)test.getAfter()).get("id");
            test.getAfter().put("_id", id);
            test.getAfter().remove("id");
        }

        if (test.getTs_ms() > mongoService.getLastUpdate("mongo")) {
            switch (test.getOp()) {
                case "c": //create
                mongoService.kafkaDataInsert("data_examplesql", test);
                    break;
                case "r": //read
                    break; //noop
                case "u": //update
                mongoService.kafkaDataUpdate("data_examplesql", test);
                    break;
                case "d": //delete
                mongoService.kafkaDataDelete("data_examplesql", test);
                    break;
            }
            mongoService.setLastUpdate(test.getTs_ms(), "mongo");
        }
    }
}
