package org.disertatie.dbsync.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Data {
    List<Map<String, Object>> rows = new ArrayList<>();


    public List<Map<String,Object>> getRows() {
        return this.rows;
    }

    public void setRows(List<Map<String,Object>> rows) {
        this.rows = rows;
    }

}
