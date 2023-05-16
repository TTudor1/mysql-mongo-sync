package org.disertatie.dbsync.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.disertatie.dbsync.common.CaputureKafkaEvent;
import org.disertatie.dbsync.common.CaputureKafkaEventMap;
import org.disertatie.dbsync.common.CaputureKafkaMongoEvent;
import org.disertatie.dbsync.common.DeserializerProvider;
import org.disertatie.dbsync.common.TableUpdateTiming;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.disertatie.dbsync.sql.DataExampleSQL;
import org.disertatie.dbsync.sql.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.mysql.cj.util.StringUtils;

import io.debezium.serde.DebeziumSerdes;


@Service
public class KafkaMongoConsumer {

    @Autowired
    MySQLService sqlService;
    @Autowired
    MyMongoService mongoService;
    @Autowired
    DeserializerProvider serde;

    private static final String prefix = "sqlTopic1";
    @KafkaListener(topics = prefix + ".test.data_examplesql", groupId = "test-group")
    public void consume(ConsumerRecord<String, String> payload) {

        System.out.println("-------MONGO RECORD-------");
        // System.out.println(payload.key());
        // System.out.println(payload.headers());
        // System.out.println(payload.partition());
        byte[] payloadBytes =  payload.value() == null ? null :  payload.value().getBytes();
        CaputureKafkaMongoEvent test = serde.getMongoDeserializer(DataExampleSQL.class).deserialize(prefix, payloadBytes);
        if (test != null) {
            System.out.println(test.getTs_ms() + " " + test.getOp());
        } else {
            System.out.println("null object");
            return;
        }

        Serde<DataExampleSQL> sd2 = DebeziumSerdes.payloadJson(DataExampleSQL.class);
        Map<String, Object> props = new HashMap<>();
        props.put("unknown.properties.ignored", true);
        // props.put("from.field", "after");
        sd2.configure(props, false);

        DataExampleSQL test2 = sd2.deserializer().deserialize(prefix, test.getAfter().getBytes());
        if (test != null) {
            // System.out.println(test.getTs_ms() + " " + test.getOp());
        } else {
            System.out.println("null object");
            return;
        }
        //TODO get table schema, get field types, deserialize event, deserialize object, store last processed message, figure out how to not have an infinite loop when syncing DBs 
        // doar manual sync - verify not to insert an already existing obj, not to update an object with the field already updated, not to delete a non exsiting object

        // if(test.getBefore()!= null) {
        //     String id = test.getBefore().get("id");
        //     if (!StringUtils.isNullOrEmpty(id)){
        //         test.getBefore().put("_id", id);
        //         test.getBefore().remove("id");
        //     }
        // }
        // if(test.getAfter()!= null) {
        //     String id = test.getAfter().get("id");
        //     if (!StringUtils.isNullOrEmpty(id)){
        //         test.getAfter().put("_id", id);
        //         test.getAfter().remove("id");
        //     }
        // }

        // if (test.getTs_ms() > TableUpdateTiming.getLastUpdated()) {
        //     switch (test.getOp()) {
        //         case "c": //create
        //         // sqlService.insertRecord("data_examplesql", test.getAfter());
        //         // mongoService.kafkaDataInsert("data_examplesql", test);
        //             break;
        //         case "u": //update
        //         // mongoService.kafkaDataUpdate("data_examplesql", test);
        //             break;
        //         case "d": //delete
        //         // mongoService.kafkaDataDelete("data_examplesql", test);
        //             break;
        //     }
        // }
    }
}
