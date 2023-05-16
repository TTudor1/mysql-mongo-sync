package org.disertatie.dbsync.sql;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

@Entity
public class DataExampleSQL {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;
    private String _id;
    private String name;
    private int quantity;


    public DataExampleSQL() {
    }

    public DataExampleSQL(String name, int quantity) {
        this.name = name;
        this.quantity = quantity;
    }

    public String get_id() {
        return this._id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public int getId() {
        return this.id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getQuantity() {
        return this.quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }
}
