package org.disertatie.dbsync.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.disertatie.dbsync.kafka.model.CaputureKafkaEventMongo;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.disertatie.dbsync.sql.MySQLService;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("unchecked")
public class KafkaMongoChangeConsumer {
    
    MySQLService sqlService;
    MyMongoService mongoService;
    String sqlTable;

    public KafkaMongoChangeConsumer(String sqlTable, MySQLService sqlService, MyMongoService mongoService) {
        this.sqlService = sqlService;
        this.mongoService = mongoService;
        this.sqlTable = sqlTable;
    }

    public void consume(ConsumerRecord<String, String> kafkaPayload) {
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
        if (payloadKey == null) {
            return;
        }

        System.out.println("MONGO RECORD CHANGE (apl to sql) table:" + "" + "id=" + payloadKey.getPayload().getId() + " " + payloadValue.getPayload().getOp());

        Map<String, Object> beforeMap = (Map<String, Object>) before;
        Map<String, Object> afterMap = (Map<String, Object>) after;

        if(beforeMap != null) {
            Integer id = (Integer) beforeMap.get("_id");
            beforeMap.put("id", id);
            beforeMap.remove("_id");
            beforeMap.remove("_class");
        }
        if(afterMap != null) {
            Integer id = (Integer)afterMap.get("_id");
            afterMap.put("id", id);
            afterMap.remove("_id");
            afterMap.remove("_class");
        }
        
        if (payloadValue.getPayload().getTs_ms() >= mongoService.getLastUpdate("sql")) {
            switch (payloadValue.getPayload().getOp()) {
                case "c": //create
                    sqlService.insertRecord(sqlTable, afterMap);
                    break;
                case "r": //read - only when doing snapshot due to topic errors
                    break; //noop
                case "u": //update
                    if (!Objects.deepEquals(afterMap, beforeMap)) {
                        sqlService.updateRecord(sqlTable, afterMap);
                    }
                    break;
                case "d": //delete
                    if (payloadKey != null) {
                        sqlService.deleteRecord(sqlTable, payloadKey.getPayload().getId());
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
