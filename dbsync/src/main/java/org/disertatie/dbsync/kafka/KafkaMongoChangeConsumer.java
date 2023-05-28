package org.disertatie.dbsync.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.disertatie.dbsync.common.DeserializerProvider;
import org.disertatie.dbsync.common.event.CaputureKafkaEventMongo;
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
@SuppressWarnings("unchecked")
public class KafkaMongoChangeConsumer {

    @Autowired
    MySQLService sqlService;
    @Autowired
    MyMongoService mongoService;
    @Autowired
    DeserializerProvider serde;

    private static final String prefix = "trt2";
    @KafkaListener(topics = prefix + ".test.data_examplesql", groupId = "test-group")
    public void consume(ConsumerRecord<String, String> kafkaPayload) {

        System.out.println("-------DETECTED MONGO RECORD CHANGE-------");
        byte[] payloadValueBytes =  kafkaPayload.value() == null ? null :  kafkaPayload.value().getBytes();
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  
        CaputureKafkaEventMongo payloadValue = null;
        CaputureKafkaEventMongo payloadKey = null;
        Object before = null;
        Object after = null;

        if (payloadValueBytes == null) {
            return;
        }

        try {
            payloadKey = mapper.readValue(kafkaPayload.key().getBytes(), CaputureKafkaEventMongo.class);
            payloadValue = mapper.readValue(payloadValueBytes, CaputureKafkaEventMongo.class);
            if (payloadValue.getPayload().getBefore() != null) {
                before = mapper.readValue(payloadValue.getPayload().getBefore().getBytes(), Object.class);
            }
            if (payloadValue.getPayload().getAfter() != null) {
                after = mapper.readValue(payloadValue.getPayload().getAfter().getBytes(), Object.class);
            }
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

        System.out.println("id=" + payloadKey.getPayload().getId());
        // System.out.println(test.getTs_ms() + " " + test.getOp());

        Map<String, Object> beforeMap = (Map<String, Object>) before;
        Map<String, Object> afterMap = (Map<String, Object>) after;

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
        

        if (payloadValue.getPayload().getTs_ms() >= mongoService.getLastUpdate("sql")) {
            switch (payloadValue.getPayload().getOp()) {
                case "c": //create
                    sqlService.insertRecord("data_examplesql", afterMap);
                    break;
                case "r": //read - only when doing snapshot due to topic errors
                    break; //noop
                case "u": //update
                    if (!Objects.deepEquals(afterMap, beforeMap)) {
                        sqlService.updateRecord("data_examplesql", afterMap);
                    }
                    break;
                case "d": //delete
                    if (payloadKey != null) {
                        sqlService.deleteRecord("data_examplesql", payloadKey.getPayload().getId());
                    }
                    break;
            }
        }
    }
}
