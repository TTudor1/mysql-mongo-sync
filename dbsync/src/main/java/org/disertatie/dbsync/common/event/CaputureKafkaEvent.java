package org.disertatie.dbsync.common.event;

public class CaputureKafkaEvent {
    private Schema schema;
    private Payload payload;

    public Payload getPayload() {
        return this.payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
