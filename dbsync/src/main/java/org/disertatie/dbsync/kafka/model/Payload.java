package org.disertatie.dbsync.kafka.model;

import java.util.Map;

public class Payload {
    private Map<String,Object> before;
    private Map<String,Object> after;
    private Source source;
    private String op;
    private long ts_ms;
    private String id;


    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String,Object> getBefore() {
        return this.before;
    }

    public void setBefore(Map<String,Object> before) {
        this.before = before;
    }

    public Map<String,Object> getAfter() {
        return this.after;
    }

    public void setAfter(Map<String,Object> after) {
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
