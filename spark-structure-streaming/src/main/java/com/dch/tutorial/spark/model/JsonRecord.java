package com.dch.tutorial.spark.model;

import java.io.Serializable;

/**
 * Class that store common information to help processes the record.
 *
 * @author david.christianto
 */
public class JsonRecord implements Serializable {

    private String record;
    private boolean deleted;

    public JsonRecord() {
    }

    public JsonRecord(String record, boolean deleted) {
        this.record = record;
        this.deleted = deleted;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return "JsonRecord{" +
                "record='" + record + '\'' +
                ", deleted=" + deleted +
                '}';
    }
}
