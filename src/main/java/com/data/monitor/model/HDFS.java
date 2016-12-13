package com.data.monitor.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Victor on 16-12-5.
 */
public class HDFS extends Result {
    private Map<String, Map<String, List<Long>>> hdfsCount;

    public HDFS() {
        this.hdfsCount = new HashMap<String, Map<String, List<Long>>>();
    }

    public Map<String, Map<String, List<Long>>> getHdfsCount() {
        return hdfsCount;
    }
}
