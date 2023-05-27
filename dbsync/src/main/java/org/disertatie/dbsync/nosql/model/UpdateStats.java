package org.disertatie.dbsync.nosql.model;

public class UpdateStats {
    private String id;
    private long last;
    private String dbName;

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getLast() {
        return this.last;
    }

    public void setLast(long last) {
        this.last = last;
    }

    public String getDbName() {
        return this.dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

}
