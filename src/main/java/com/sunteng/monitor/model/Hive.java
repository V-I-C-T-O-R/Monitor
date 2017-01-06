package com.sunteng.monitor.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Victor on 16-12-5.
 */
public class Hive extends Result {
    private Map<String, Long> sumCount;
    private Map<String, Map<String, Boolean>> validatPartition;
    private Map<String, Map<String, Long>> countRecords;


    public Hive() {
        this.sumCount = new HashMap<String, Long>();
        this.validatPartition = new HashMap<String, Map<String, Boolean>>();
        this.countRecords = new HashMap<String, Map<String, Long>>();
    }

    public Map<String, Map<String, Boolean>> getValidatPartition() {
        return validatPartition;
    }

    public Map<String, Map<String, Long>> getCountRecords() {
        return countRecords;
    }

    public Map<String, Long> getSumCount() {
        return sumCount;
    }
}
