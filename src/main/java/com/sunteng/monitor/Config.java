package com.sunteng.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Victor on 16-12-7.
 */
public class Config {
    private static final Logger logger = Logger.getLogger(Config.class);
    public static final String configFile = "config.json";    //项目配置文件config.json
    private static final Pattern regex = Pattern.compile("\"@extend:.*?\"");
    public static Config instance;
    @JsonProperty("datacenter.pool.max")
    public Integer poolMax;
    @JsonProperty("datacenter.pool.min")
    public Integer poolMin;
    @JsonProperty("datacenter.job.timeout")
    public Long timeOut;
    @JsonProperty("sendsubject")
    public String sendSubject;
    @JsonProperty("sendto")
    public List<String> sendTo;
    @JsonProperty("azkabanusername")
    public String azkabanUsername;
    @JsonProperty("azkabanpassword")
    public String azkabanPassword;
    @JsonProperty("azkabanport")
    public Integer azkabanPort;
    @JsonProperty("azkabanhostname")
    public String azkabanHostName;
    @JsonProperty("wechaturl")
    public String weChatUrl;
    @JsonProperty("mysqlpartitions")
    public Integer mysqlPartitions;
    @JsonProperty("tmppath")
    public String tmpPath;
    @JsonProperty
    public Map<String, String> azkaban;
    @JsonProperty
    public Map<String, List<String>> mysql;
    @JsonProperty
    public Map<String, List<String>> hive;
    @JsonProperty
    public Map<String, List<String>> hdfs;

    public static boolean initConfig() throws Exception {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
        if (inputStream == null) {
            return false;
        }
        ObjectMapper jsonMapper = new ObjectMapper();
        Config config = jsonMapper.readValue(readStream(inputStream), Config.class);
        instance = config;
        return true;
    }

    public static String readStream(InputStream inputStream) throws IOException {
        String content = IOUtils.toString(inputStream);
        Matcher matcher = regex.matcher(content);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String reg = matcher.group();
            String replaceFile = StringUtils.removeEnd(StringUtils.removeStart(reg, "\"@extend:"), "\"").trim();
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(replaceFile);
            String replaceJson = readStream(in);
            matcher.appendReplacement(sb, replaceJson);
            in.close();
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public Integer getAzkabanPort() {
        return azkabanPort;
    }

    public String getAzkabanHostName() {
        return azkabanHostName;
    }

    public String getTmpPath() {
        return tmpPath;
    }

    public Integer getMysqlPartitions() {
        return mysqlPartitions;
    }

    public String getWeChatUrl() {
        return weChatUrl;
    }

    public Map<String, List<String>> getMysql() {
        return mysql;
    }

    public String getAzkabanPassword() {
        return azkabanPassword;
    }

    public String getAzkabanUsername() {
        return azkabanUsername;
    }

    public String getSendSubject() {
        return sendSubject;
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
