package com.sunteng.monitor.model;

import com.sunteng.monitor.service.ServiceCenter;

import org.apache.log4j.Logger;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by Victor on 16-12-5.
 */
public class QueryResult implements Callable<Result> {
    private static final Logger logger = Logger.getLogger(QueryResult.class);
    private String[] flags;
    private ServiceCenter service;
    public static Map<String, Object> results = new Hashtable<String, Object>();

    public QueryResult(String[] flags, ServiceCenter service) {
        this.flags = flags;
        this.service = service;
    }

    public QueryResult(ServiceCenter service) {
        this.service = service;
    }

    @Override
    public Result call() throws Exception {
        Result result = this.service.getServiceStatus(this.flags);
        if ("azkaban".equals(result.getType())) {
            Azkaban azkaban = (Azkaban) result;
            results.put(azkaban.getType(), azkaban);
        } else if ("hive".equals(result.getType())) {
            Hive hive = (Hive) result;
            results.put(hive.getType(), hive);
        } else if ("hdfs".equals(result.getType())) {
            HDFS hdfs = (HDFS) result;
            results.put(hdfs.getType(), hdfs);
        }
        return result;
    }

    public ServiceCenter getService() {
        return service;
    }

    public void setService(ServiceCenter service) {
        this.service = service;
    }

    public Map<String, Object> getResults() {
        return results;
    }

    public void setResults(Map<String, Object> results) {
        this.results = results;
    }

}
