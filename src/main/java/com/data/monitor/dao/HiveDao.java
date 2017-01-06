package com.data.monitor.dao;

import com.data.monitor.Config;
import com.data.monitor.model.Hive;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Victor on 16-12-2.
 */
public class HiveDao {
    private static final Logger logger = Logger.getLogger(HiveDao.class);

    public static Hive getHiveMap(String startTime, String endTime, String[] flags) throws Exception {
        Map<String, List<String>> tableBase = null;
        Hive hive = new Hive();
        tableBase = Config.instance.getHive();
        for (Map.Entry<String, List<String>> entry : tableBase.entrySet()) {
            for (String table : entry.getValue()) {
                long l = 0;
                Map<String, Long> map = new HashMap<String, Long>();
                for (String flag : flags) {
                    StringBuffer sb = new StringBuffer(getSql(entry.getKey(), table));
                    sb.append(" where dt = ").append(flag);
                    long c = count(sb.toString().trim());
                    map.put(flag, c);
                    l += c;
                }
                hive.getCountRecords().put(table, map);
                hive.getSumCount().put(table, l);
                Map<String, Boolean> partition = new HashMap<String, Boolean>();
                for (int i = 0; i < flags.length; i++) {
                    partition.put(flags[i], validatePartition(entry.getKey(), table, flags[i]));
                }
                hive.getValidatPartition().put(table, partition);
            }
        }
        return hive;
    }

    private static String getSql(String database, String tableName) {
        StringBuffer sql = new StringBuffer("select count(1) from ").append(database).append(".").append(tableName);
        return sql.toString();
    }

    public static long count(String sql) {
        logger.info(sql);
        Connection con = null;
        ResultSet resultSet = null;
        Statement stmt = null;
        try {
            con = Connections.getInstance().getHiveConn().getConnection();
            stmt = con.createStatement();
            resultSet = stmt.executeQuery(sql);
            if (resultSet.next()) {
                long cnt = resultSet.getLong(1);
                logger.info("result: cnt : " + cnt);
                return cnt;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getClass().getName() + ":" + e.getMessage());
            logger.error("executing sql:" + sql);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    public static boolean validatePartition(String database, String table, String flag) {
        ResultSet resultSet = null;
        Connection con = null;
        Statement stmt = null;
        boolean mark = false;
        try {
            con = Connections.getInstance().getHiveConn().getConnection();
            stmt = con.createStatement();
            StringBuffer sb = new StringBuffer("show partitions ").append(database).append(".").append(table);
            resultSet = stmt.executeQuery(sb.toString());
            while (resultSet.next()) {
                String part = resultSet.getString(1);
                if (("dt=" + flag).equals(part)) {
                    mark = true;
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getClass().getName() + ":" + e.getMessage());
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return mark;
    }
}
