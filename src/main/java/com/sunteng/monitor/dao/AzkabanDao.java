package com.sunteng.monitor.dao;

import com.sunteng.monitor.Config;
import com.sunteng.monitor.model.Azkaban;
import com.sunteng.monitor.utils.ToolUtils;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Victor on 16-12-2.
 */
public class AzkabanDao {
    private static final Logger logger = Logger.getLogger(AzkabanDao.class);

    public static Azkaban getAzkabanList(String beginTime, String endTime) {
        Connection conn = null;
        ResultSet resultSet = null;
        Statement stmt = null;
        Map<String, String> tableAttr = null;
        Azkaban azkaban = new Azkaban();
        if (endTime != null) {
            endTime = String.valueOf(Integer.valueOf(endTime) + 1);
        }
        try {
            conn = Connections.getInstance().getAzkabanConn().getConnection();
            stmt = conn.createStatement();
            tableAttr = Config.instance.getAzkaban();
            for (Map.Entry<String, String> entry : tableAttr.entrySet()) {
                StringBuffer sb = null;
                if ("execution_jobs".equals(entry.getKey())) {
                    String[] attr = entry.getValue().split(",");
                    if (beginTime != null && endTime != null) {
                        sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(" where ").append(attr[attr.length - 2]).append(" >= ").append(ToolUtils.DF.parse(beginTime).getTime()).append(" and ").append(attr[attr.length - 1]).append(" <= ").append(ToolUtils.DF.parse(endTime).getTime()).append(";");
                    } else {
                        if (beginTime != null) {
                            sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(" where ").append(attr[attr.length - 2]).append(" >= ").append(ToolUtils.DF.parse(beginTime).getTime()).append(";");
                        }
                        if (endTime != null) {
                            sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(" where ").append(attr[attr.length - 1]).append(" <= ").append(ToolUtils.DF.parse(endTime).getTime()).append(";");
                        }
                    }

                    resultSet = stmt.executeQuery(sb.toString());
                    while (resultSet.next()) {
                        int execId = resultSet.getInt(1);
                        int projectId = resultSet.getInt(2);
                        String flowId = resultSet.getString(3);
                        String jobId = resultSet.getString(4);
                        int attempt = resultSet.getInt(5);
                        int status = resultSet.getInt(6);
                        String inputParams = resultSet.getString(7);
                        String outputParams = resultSet.getString(8);
                        String startTTime = resultSet.getString(9);
                        String endTTime = resultSet.getString(10);
                        Azkaban.Azkabanjobs azkabanjobs = azkaban.new Azkabanjobs(execId, attempt, endTTime, flowId, inputParams, jobId, outputParams, projectId, startTTime, status);
                        azkaban.putAzkabanjobs(azkabanjobs);
                    }
                }
                if ("execution_flows".equals(entry.getKey())) {
                    String[] attr = entry.getValue().split(",");
                    if (beginTime != null && endTime != null) {
                        sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(" where ").append(attr[attr.length - 2]).append(" >= ").append(ToolUtils.DF.parse(beginTime).getTime()).append(" and ").append(attr[attr.length - 1]).append(" <= ").append(ToolUtils.DF.parse(endTime).getTime()).append(";");
                    } else {
                        if (beginTime != null) {
                            sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(" where ").append(attr[attr.length - 2]).append(" >= ").append(ToolUtils.DF.parse(beginTime).getTime()).append(";");
                        }
                        if (endTime != null) {
                            sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(" where ").append(attr[attr.length - 1]).append(" <= ").append(ToolUtils.DF.parse(endTime).getTime()).append(";");
                        }
                    }
                    resultSet = stmt.executeQuery(sb.toString());
                    while (resultSet.next()) {
                        int execId = resultSet.getInt(1);
                        int projectId = resultSet.getInt(2);
                        String flowId = resultSet.getString(3);
                        int status = resultSet.getInt(4);
                        String submitUser = resultSet.getString(5);
                        String submitTime = resultSet.getString(6);
                        String updateTime = resultSet.getString(7);
                        String startTTime = resultSet.getString(8);
                        String endTTime = resultSet.getString(9);
                        Azkaban.Azkabanflows azkabanflows = azkaban.new Azkabanflows(execId, endTTime, flowId, projectId, startTTime, status, submitTime, submitUser, updateTime);
                        azkaban.putAzkabanflows(azkabanflows);
                    }
                }
                if ("projects".equals(entry.getKey())) {
                    sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(";");
                    resultSet = stmt.executeQuery(sb.toString());
                    while (resultSet.next()) {
                        int projectId = resultSet.getInt(1);
                        String projectName = resultSet.getString(2);
                        int active = resultSet.getInt(3);
                        String modifiedTime = resultSet.getString(4);
                        String createTime = resultSet.getString(5);
                        String lastModifiedUser = resultSet.getString(6);
                        Azkaban.Azkabanprojects azkabanprojects = azkaban.new Azkabanprojects(active, createTime, lastModifiedUser, modifiedTime, projectName, projectId);
                        azkaban.putAzkabanprojects(azkabanprojects);
                    }
                }
                if ("schedules".equals(entry.getKey())) {
                    sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(";");
                    resultSet = stmt.executeQuery(sb.toString());
                    while (resultSet.next()) {
                        int projectId = resultSet.getInt(1);
                        String projectName = resultSet.getString(2);
                        String flowName = resultSet.getString(3);
                        String status = resultSet.getString(4);
                        String firstExecTime = resultSet.getString(5);
                        String period = resultSet.getString(6);
                        String lastModifyTime = resultSet.getString(7);
                        String nextExecTime = resultSet.getString(8);
                        String submitTime = resultSet.getString(9);
                        String submitUser = resultSet.getString(10);
                        Azkaban.Azkabanschedules azkabanschedules = azkaban.new Azkabanschedules(firstExecTime, flowName,
                                lastModifyTime, nextExecTime, period, projectId, projectName, status, submitTime, submitUser);
                        azkaban.putAzkabanschedules(azkabanschedules);
                    }
                }
                if ("active_executing_flows".equals(entry.getKey())) {
                    sb = new StringBuffer("select ").append(entry.getValue()).append(" from ").append(entry.getKey()).append(";");
                    resultSet = stmt.executeQuery(sb.toString());
                    while (resultSet.next()) {
                        int execId = resultSet.getInt(1);
                        Azkaban.AzkabansExecutingflows azkabansExecutingflows = azkaban.new AzkabansExecutingflows(execId);
                        azkaban.putAzkabansExecutingflowses(azkabansExecutingflows);
                    }
                }
            }

        } catch (Exception e) {
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
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return azkaban;
    }

    public static Map<String, Map<String, Boolean>> getStatPartition(List<String> tableNames, List<String> flag) {
        Connection conn = null;
        ResultSet resultSet = null;
        Statement stmt = null;
        Map<String, Map<String, Boolean>> partition = new HashMap<String, Map<String, Boolean>>();
        try {
            conn = Connections.getInstance().getMysqlConn().getConnection();
            stmt = conn.createStatement();
            for (String tableName : tableNames) {
                List<String> part = new ArrayList<String>();
                StringBuffer sb = new StringBuffer("select PARTITION_NAME ").append(" from ").append(" INFORMATION_SCHEMA.PARTITIONS ").append("where TABLE_NAME = '").append(tableName).append("';");
                resultSet = stmt.executeQuery(sb.toString());
                Map<String, Boolean> map = new HashMap<String, Boolean>();
                for (String f : flag) {
                    boolean mark = false;
                    while (resultSet.next()) {
                        String name = resultSet.getString(1);
                        String p = "p" + f;
                        if (p.equals(name)) {
                            mark = true;
                            map.put(f, mark);
                            break;
                        }
                    }
                    if (!mark) {
                        map.put(f, mark);
                    }
                }
                partition.put(tableName, map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return partition;
    }
}
