package org.disertatie.dbsync.common;

import java.util.Map;

public class CaputureKafkaMongoEvent {
    private String before;
    private String after;
    private Source source;
    private String op;
    private long ts_ms;

    public String getBefore() {
        return this.before;
    }

    public void setBefore(String before) {
        this.before = before;
    }

    public String getAfter() {
        return this.after;
    }

    public void setAfter(String after) {
        this.after = after;
    }

    public Source getSource() {
        return this.source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public String getOp() {
        return this.op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public long getTs_ms() {
        return this.ts_ms;
    }

    public void setTs_ms(long ts_ms) {
        this.ts_ms = ts_ms;
    }
}
