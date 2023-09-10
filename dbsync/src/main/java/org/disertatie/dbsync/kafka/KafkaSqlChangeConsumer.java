package org.disertatie.dbsync.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.disertatie.dbsync.kafka.model.CaputureKafkaEvent;
import org.disertatie.dbsync.kafka.model.Payload;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.disertatie.dbsync.sql.MySQLService;
import org.springframework.dao.DataIntegrityViolationException;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaSqlChangeConsumer implements KafkaChangeConsumer {

    MySQLService sqlService;
    MyMongoService mongoService;
    String collectionName;
    private int MAX_ATTEMPTS = 5;
    boolean debug = false;

    public KafkaSqlChangeConsumer(String collectionName, MySQLService sqlService, MyMongoService mongoService) {
        this.sqlService = sqlService;
        this.mongoService = mongoService;
        this.collectionName = collectionName;
    }

    public void consume(ConsumerRecord<String, String> kafkaPayload) throws InterruptedException{

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
            System.out.println("EERRRRRROOOORRRR1");
            return;
        }
        if (payloadKey == null) {
            System.out.println("EERRRRRROOOORRRR2");
            return;
        }

        Payload payload = payloadValue.getPayload();
        
        if (debug) {
            System.out.println("SQL RECORD CHANGE (apl to mongo) table:" + collectionName + "id=" + payloadKey.getPayload().getId() + " " + payload.getOp());
        }
        if(payload.getBefore() != null) {
            Integer id = (Integer) payload.getBefore().get("id");
                payload.getBefore().put("_id", id);
                payload.getBefore().remove("id");
        }
        if(payload.getAfter() != null) {
            Integer id = (Integer)((Map<String,Object>)payload.getAfter()).get("id");
            payload.getAfter().put("_id", id);
            payload.getAfter().remove("id");
        }

        // if (payload.getTs_ms() >= mongoService.getLastUpdate("mongo")) {
            boolean success = false;
            int attempt = 0;
            do {
                try {
                    attempt++;
                    attemptOperation(payload); 
                    success = true;
                } catch (DataIntegrityViolationException e) {
                    System.out.println("Attempt " + attempt + " Foreign key constraint failure in table " 
                    + collectionName +" id:" + payloadKey.getPayload().getId());
                    e.printStackTrace();
                    Thread.sleep(1000*(int)Math.pow(2, attempt));
                }
            } while (!success && attempt < MAX_ATTEMPTS);

        // } else {
        //     System.out.println("Message dropped tsms:" + payload.getTs_ms() + " dbTime:" + mongoService.getLastUpdate("mongo"));
        // }
    }

    private void attemptOperation(Payload payload) {
            switch (payload.getOp()) {
                case "c": //create
                // System.out.println(collectionName + " " + payload.getAfter().get("_id"));
                mongoService.kafkaDataInsert(collectionName, payload);
                    break;
                case "r": //read - only when doing snapshot due to topic errors
                    break; //noop
                case "u": //update
                    if (!Objects.deepEquals(payload.getAfter(), payload.getBefore())) {
                        mongoService.kafkaDataUpdate(collectionName, payload);
                    }
                    break;
                case "d": //delete
                mongoService.kafkaDataDelete(collectionName, payload);
                    break;
            }
            mongoService.setLastUpdate(payload.getTs_ms(), "mongo");
    }
}
