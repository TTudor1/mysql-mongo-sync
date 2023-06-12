package org.disertatie.dbsync.kafka.model;

public class CaputureKafkaEventMongo {
    private Schema schema;
    private PayloadMongo payload;

    public PayloadMongo getPayload() {
        return this.payload;
    }

    public void setPayload(PayloadMongo payload) {
        this.payload = payload;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
