package org.disertatie.dbsync.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Schema {
    private String name;
    private List<Map<String, String>> fields = new ArrayList<>();

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Map<String,String>> getFields() {
        return this.fields;
    }

    public void setFields(List<Map<String,String>> fields) {
        this.fields = fields;
    }

}
