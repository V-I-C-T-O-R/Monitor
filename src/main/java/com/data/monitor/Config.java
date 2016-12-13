package com.data.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.data.monitor.utils.FileUtil;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by Victor on 16-12-7.
 */
public class Config {
    private static final Logger logger = Logger.getLogger(Config.class);
    public static Config instance;

    @JsonProperty("datacenter.pool.max")
    public Integer poolMax;
    @JsonProperty("datacenter.pool.min")
    public Integer poolMin;
    @JsonProperty("datacenter.job.timeout")
    public Long timeOut;
    @JsonProperty("sendfrom")
    public String sendFrom;
    @JsonProperty("sendsubject")
    public String sendSubject;
    @JsonProperty("sendto")
    public List<String> sendTo;
    @JsonProperty
    public Map<String, String> azkaban;
    @JsonProperty
    public Map<String, List<String>> hive;
    @JsonProperty
    public Map<String, List<String>> hdfs;

    public static void initConfig(String configDir) throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        Config config = jsonMapper.readValue(FileUtil.readJsonFile(configDir + "/config.json"), Config.class);
        instance = config;
    }

    public String getSendSubject() {
        return sendSubject;
    }

    public String getSendFrom() {
        return sendFrom;
    }

    public List<String> getSendTo() {
        return sendTo;
    }

    public Map<String, String> getAzkaban() {
        return azkaban;
    }

    public Map<String, List<String>> getHdfs() {
        return hdfs;
    }

    public Map<String, List<String>> getHive() {
        return hive;
    }

    public Integer getPoolMax() {
        return poolMax;
    }

    public Integer getPoolMin() {
        return poolMin;
    }

    public Long getTimeOut() {
        return timeOut;
    }
}
