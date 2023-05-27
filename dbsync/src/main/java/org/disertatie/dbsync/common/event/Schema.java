package org.disertatie.dbsync.common.event;

import java.util.List;

public class Schema {
    private String type;
    private List<Schema> fields;
    private boolean optional;
    private String name;
    private String field;
    private int version;

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Schema> getFields() {
        return this.fields;
    }

    public void setFields(List<Schema> fields) {
        this.fields = fields;
    }

    public boolean isOptional() {
        return this.optional;
    }

    public boolean getOptional() {
        return this.optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getField() {
        return this.field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public int getVersion() {
        return this.version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
