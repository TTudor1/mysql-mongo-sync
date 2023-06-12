package org.disertatie.dbsync.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.disertatie.dbsync.common.Data;
import org.disertatie.dbsync.common.Schema;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

@Service
public class MySQLService {
    
    // @Autowired
    // private DataExampleSQLRepository repository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private MyMongoService mongoService;
    
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

    public void insertRecord(String tableName, Map<String,Object> values) {
        if (!mongoService.existsWithId((int)values.get("id"), tableName)) {
            return;
        }
        String query = "select id from " + tableName + " where id = '" + values.get("id") + "'";
        List<Object> res = jdbcTemplate.query(query, new RowMapper<Object>(){
            @Override
            @Nullable
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                return null;
            }
        });
        if (!res.isEmpty()) {
            return;
        }
        String keys = values.keySet().stream().reduce("", (a, b) -> a + b + ",");
        String vals = values.values().stream().map(v -> v.toString()).reduce("", (a, b) -> a + "'" + b + "',");
        
        query = "insert into " + tableName + " (" + removeLastChar(keys) + ") values (" + removeLastChar(vals) + ")";
        try {
            jdbcTemplate.execute(query);
        } catch (DataIntegrityViolationException e) {
            System.out.println("Foreign key constraint failure");
        }
    }

    public void updateRecord(String tableName, Map<String,Object> values) {
        String query = "select id from " + tableName + " where id = '" + values.get("id") + "'";
        List<Object> res = jdbcTemplate.query(query, new RowMapper<Object>(){
            @Override
            @Nullable
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                return null;
            }
        });
        if (res.isEmpty()) {
            return;
        }
        String vals = values.keySet().stream().map(v -> v + " = '" + values.get(v) + "'").reduce("", (a, v) -> a + v + ",");
                        
        query = "update " + tableName + " set " + removeLastChar(vals) + " where id = " + values.get("id");
        try {
            jdbcTemplate.execute(query);
        } catch (DataIntegrityViolationException e) {
            System.out.println("Foreign key constraint failure");
        }
    }

    public void deleteRecord(String tableName, String id) {
        String query = "select id from " + tableName + " where id = '" + id + "'";
        List<Object> res = jdbcTemplate.query(query, new RowMapper<Object>(){
            @Override
            @Nullable
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                return null;
            }
        });
        if (res.isEmpty()) {
            return;
        }
        
        query = "delete from " + tableName + " where id =" + id;
        try {
            jdbcTemplate.execute(query);
        } catch (DataIntegrityViolationException e) {
            System.out.println("Foreign key constraint failure");
        }
    }

    public static String removeLastChar(String s) {
        return (s == null || s.length() == 0)
          ? null 
          : (s.substring(0, s.length() - 1));
    }
}
