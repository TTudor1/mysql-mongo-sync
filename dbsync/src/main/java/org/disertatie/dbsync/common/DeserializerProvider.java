package org.disertatie.dbsync.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.stereotype.Service;

import io.debezium.serde.DebeziumSerdes;

@Service
public class DeserializerProvider {
    
    Map<Class<?>, Serde<CaputureKafkaEvent>> deserializers = new HashMap<>();
    Map<Class<?>, Serde<CaputureKafkaMongoEvent>> mongoDeserializers = new HashMap<>();

    public <T> Deserializer<CaputureKafkaEvent> getDeserializer(Class<T> clazz) {
        if (deserializers.get(clazz) == null) {
            Serde<CaputureKafkaEvent> sd = DebeziumSerdes.payloadJson(CaputureKafkaEvent.class);
            Map<String, Object> props = new HashMap<>();
            props.put("unknown.properties.ignored", true);
            // props.put("from.field", "after");
            sd.configure(props, false);
            deserializers.put(clazz, sd);
            return sd.deserializer();
        }
        return ((Serde<CaputureKafkaEvent>) deserializers.get(clazz)).deserializer();
    }

    public <T> Deserializer<CaputureKafkaMongoEvent> getMongoDeserializer(Class<T> clazz) {
        if (mongoDeserializers.get(clazz) == null) {
            Serde<CaputureKafkaMongoEvent> sd = DebeziumSerdes.payloadJson(CaputureKafkaMongoEvent.class);
            Map<String, Object> props = new HashMap<>();
            props.put("unknown.properties.ignored", true);
            // props.put("from.field", "after");
            sd.configure(props, false);
            mongoDeserializers.put(clazz, sd);
            return sd.deserializer();
        }
        return ((Serde<CaputureKafkaMongoEvent>) mongoDeserializers.get(clazz)).deserializer();
    }
}
