package org.disertatie.dbsync.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.disertatie.dbsync.common.CaputureKafkaEvent;
import org.disertatie.dbsync.common.DeserializerProvider;
import org.disertatie.dbsync.common.TableUpdateTiming;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.disertatie.dbsync.sql.DataExampleSQL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.mysql.cj.util.StringUtils;


@Service
public class KafkaSqlConsumer {

    @Autowired
    MyMongoService mongoService;
    @Autowired
    DeserializerProvider serde;

    private static final String prefix = "trt";
    @KafkaListener(topics = prefix + ".db_example.data_examplesql", groupId = "test-group")
    public void consume(ConsumerRecord<String, String> payload) {

        System.out.println("--------SQL RECORD--------");
        // System.out.println(payload.key());
        // System.out.println(payload.headers());
        // System.out.println(payload.partition());
        byte[] payloadBytes =  payload.value() == null ? null :  payload.value().getBytes();
        CaputureKafkaEvent test = serde.getDeserializer(DataExampleSQL.class).deserialize(prefix, payloadBytes);
        if (test != null) {
            System.out.println(test.getTs_ms() + " " + test.getOp());
        } else {
            System.out.println("null object");
            return;
        }
        if(test.getBefore()!= null) {
            String id = test.getBefore().get("id");
            if (!StringUtils.isNullOrEmpty(id)){
                test.getBefore().put("_id", id);
                test.getBefore().remove("id");
            }
        }
        if(test.getAfter()!= null) {
            String id = test.getAfter().get("id");
            if (!StringUtils.isNullOrEmpty(id)){
                test.getAfter().put("_id", id);
                test.getAfter().remove("id");
            }
        }

        if (test.getTs_ms() > TableUpdateTiming.getLastUpdated()) {
            switch (test.getOp()) {
                case "c": //create
                mongoService.kafkaDataInsert("data_examplesql", test);
                    break;
                case "u": //update
                mongoService.kafkaDataUpdate("data_examplesql", test);
                    break;
                case "d": //delete
                mongoService.kafkaDataDelete("data_examplesql", test);
                    break;
            }
        }
    }
}
