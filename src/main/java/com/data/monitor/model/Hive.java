package com.data.monitor.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Victor on 16-12-5.
 */
public class Hive extends Result {
    private Map<String, Long> countRecords;
    private Map<String, Map<String, Boolean>> validatPartition;

    public Hive() {
        this.countRecords = new HashMap<String, Long>();
        this.validatPartition = new HashMap<String, Map<String, Boolean>>();
    }

    public Map<String, Map<String, Boolean>> getValidatPartition() {
        return validatPartition;
    }

    public Map<String, Long> getCountRecords() {
        return countRecords;
    }
}
