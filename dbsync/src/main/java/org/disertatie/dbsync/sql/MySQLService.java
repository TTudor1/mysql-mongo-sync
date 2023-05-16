package org.disertatie.dbsync.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.disertatie.dbsync.common.Data;
import org.disertatie.dbsync.common.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Service;

@Service
public class MySQLService {
    
    @Autowired
    private DataExampleSQLRepository repository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    public List<String> getTables() {
        return jdbcTemplate.queryForList("SHOW TABLES", String.class);
    }

    public Schema getTableSchema(String tableName) {
        String sql = "SELECT * FROM " + tableName + " WHERE 1=0";
        SqlRowSet rs = jdbcTemplate.queryForRowSet(sql);
        SqlRowSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        List<Map<String, String>> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            Map<String, String> column = new HashMap<>();
            column.put("name", md.getColumnName(i));
            column.put("type", md.getColumnTypeName(i));
            System.out.println(md.getColumnName(i));
            System.out.println(md.getColumnTypeName(i));
            columns.add(column);
        }
        Schema schema = new Schema();
        schema.setName(tableName);
        schema.setFields(columns);
        return schema;
    }

    public Data getTableData(String tableName) {
        String query = "select * from " + tableName;
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);
        Data data = new Data();
        data.setRows(rows);
        return data;
    }

    public void insertRecord(String tableName, Map<String,String> values) {
        String keys = values.keySet().stream().reduce("", (a, b) -> a + " " + b);
        String vals = values.values().stream().reduce("", (a, b) -> a + " " + b);
        String query = "insert into " + tableName + " (" + keys + ") values (" + vals + ")";
        jdbcTemplate.execute(query);
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);
    }
}
