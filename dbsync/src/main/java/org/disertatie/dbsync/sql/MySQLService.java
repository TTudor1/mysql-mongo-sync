package org.disertatie.dbsync.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.disertatie.dbsync.nosql.MyMongoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

@Service
public class MySQLService {
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private MyMongoService mongoService;
    
    public List<String> getTables() {
        return jdbcTemplate.queryForList("SHOW TABLES", String.class);
    }

    public void insertRecord(String tableName, Map<String,Object> values) {
        int id = (int)values.get("id");
        if (!mongoService.existsWithId(id, tableName)) {
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

        jdbcTemplate.execute(query);

    }

    public void updateRecord(String tableName, Map<String,Object> values) {
        String id = ""+values.get("id");
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
        String vals = values.keySet().stream().map(v -> v + " = '" + values.get(v) + "'").reduce("", (a, v) -> a + v + ",");
                        
        query = "update " + tableName + " set " + removeLastChar(vals) + " where id = " + id;

        jdbcTemplate.execute(query);
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

        jdbcTemplate.execute(query);
    }

    public static String removeLastChar(String s) {
        return (s == null || s.length() == 0)
          ? null 
          : (s.substring(0, s.length() - 1));
    }
}
