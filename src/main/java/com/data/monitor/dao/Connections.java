package com.data.monitor.dao;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

/**
 * Created by Victor on 16-12-2.
 */
public class Connections {
    private static Connections instance = null;
    public static final String KAFKA = "kafka";
    public static final String NGINX = "nginx";
    private Map<String, DataSource> hiveConn = null;
    private DataSource mysqlConn = null;
    private FileSystem hdfsConn = null;
    private final String HIVE_KAFKA_PROPERTY = "hivekafka.properties";
    private final String HIVE_NGINX_PROPERTY = "hivenginx.properties";
    private final String MYSQL_PROPERTY = "mysql.properties";

    private Connections() {
        initHiveConn();
        initMysqlConn();
        initHDFSConn();
    }

    public static Connections getInstance() {
        if (instance == null) {
            synchronized (Connections.class) {
                if (instance == null) {
                    instance = new Connections();
                }
            }
        }
        return instance;
    }

    public Map<String, DataSource> getHiveConn() {
        return hiveConn;
    }

    public DataSource getMysqlConn() {
        return mysqlConn;
    }

    public FileSystem getHdfsConn() {
        return hdfsConn;
    }

    private void initHiveConn() {
        hiveConn = new HashMap<String, DataSource>();
        try {
            Properties kafkaProperties = new Properties();
            kafkaProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(HIVE_KAFKA_PROPERTY));
            DataSource hiveKafkaConn = BasicDataSourceFactory.createDataSource(kafkaProperties);
            hiveConn.put(KAFKA, hiveKafkaConn);
            Properties nginxProperties = new Properties();
            nginxProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(HIVE_NGINX_PROPERTY));
            DataSource nginxKafkaConn = BasicDataSourceFactory.createDataSource(nginxProperties);
            hiveConn.put(NGINX, nginxKafkaConn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initMysqlConn() {
        Properties dbProperties = new Properties();
        try {
            dbProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(MYSQL_PROPERTY));
            mysqlConn = BasicDataSourceFactory.createDataSource(dbProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initHDFSConn() {
        Configuration conf = new Configuration();
        conf.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("core-site.xml"));
        try {
            hdfsConn = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
