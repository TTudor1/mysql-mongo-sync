package org.disertatie.dbsync.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

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
    @KafkaListener(topics = prefix + ".db_example.data_examplesql", groupId = "test-group")
    public void consume(ConsumerRecord<String, String> kafkaPayload) {

        System.out.println("--------DETECTED SQL RECORD CHANGE--------");
        byte[] payloadValueBytes =  kafkaPayload.value() == null ? null :  kafkaPayload.value().getBytes();
        ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  
        if (payloadValueBytes == null) {
            return;
        }

        CaputureKafkaEvent payloadValue = null;
        CaputureKafkaEvent payloadKey = null;

        try {
            payloadKey = mapper.readValue(kafkaPayload.key().getBytes(), CaputureKafkaEvent.class);
            payloadValue = mapper.readValue(payloadValueBytes, CaputureKafkaEvent.class);
        } catch (StreamReadException e) {
            e.printStackTrace();
        } catch (DatabindException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (payloadValue == null) {
            return;
        }
        Payload payload = payloadValue.getPayload();
        
        System.out.println("id=" + payloadKey.getPayload().getId() + " " + payload.getOp());
        // System.out.println(test.getTs_ms() + " " + test.getOp());

        if(payload.getBefore() != null) {
            Integer id = (Integer) payload.getBefore().get("id");
                payload.getBefore().put("_id", id);
                payload.getBefore().remove("id");
        }
        if(payload.getAfter() != null) {
            Integer id = (Integer)((Map)payload.getAfter()).get("id");
            payload.getAfter().put("_id", id);
            payload.getAfter().remove("id");
        }

        if (payload.getTs_ms() >= mongoService.getLastUpdate("mongo")) {
            switch (payload.getOp()) {
                case "c": //create
                mongoService.kafkaDataInsert("data_examplesql", payload);
                    break;
                case "r": //read - only when doing snapshot due to topic errors
                    break; //noop
                case "u": //update
                    if (!Objects.deepEquals(payload.getAfter(), payload.getBefore())) {
                        mongoService.kafkaDataUpdate("data_examplesql", payload);
                    }
                    break;
                case "d": //delete
                mongoService.kafkaDataDelete("data_examplesql", payload);
                    break;
            }
            mongoService.setLastUpdate(payload.getTs_ms(), "mongo");
        } else {
            System.out.println("Message dropped tsms:" + payload.getTs_ms() + " dbTime:" + mongoService.getLastUpdate("mongo"));
        }
    }
}
