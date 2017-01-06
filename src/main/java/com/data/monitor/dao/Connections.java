package com.data.monitor.dao;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

import javax.sql.DataSource;

/**
 * Created by Victor on 16-12-2.
 */
public class Connections {
    private static final Logger logger = Logger.getLogger(Connections.class);
    private static Connections instance = null;
    private DataSource hiveConn = null;
    private DataSource mysqlConn = null;
    private DataSource azkabanConn = null;
    private FileSystem hdfsConn = null;
    private final String HIVE_PROPERTY = "hive.properties";
    private final String MYSQL_PROPERTY = "mysql.properties";
    private final String AZKABAN_PROPERTY = "azkaban.properties";

    private Connections() {
        initHiveConn();
        initMysqlConn();
        initHDFSConn();
        initAzkabanConn();
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

    public DataSource getHiveConn() {
        return hiveConn;
    }

    public DataSource getMysqlConn() {
        return mysqlConn;
    }

    public FileSystem getHdfsConn() {
        return hdfsConn;
    }
    public DataSource getAzkabanConn(){
        return azkabanConn;
    }

    private void initHiveConn() {
        Properties dbProperties = new Properties();
        try {
            dbProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(HIVE_PROPERTY));
            hiveConn = BasicDataSourceFactory.createDataSource(dbProperties);
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
    private void initAzkabanConn() {
        Properties dbProperties = new Properties();
        try {
            dbProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(AZKABAN_PROPERTY));
            azkabanConn = BasicDataSourceFactory.createDataSource(dbProperties);
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
