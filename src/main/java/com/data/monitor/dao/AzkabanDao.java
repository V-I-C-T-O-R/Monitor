package com.data.monitor.dao;

import com.data.monitor.Config;
import com.data.monitor.model.Azkaban;
import com.data.monitor.utils.ToolUtils;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
            conn = Connections.getInstance().getMysqlConn().getConnection();
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
                        int projectId = resultSet.getInt(1);
                        String flowId = resultSet.getString(2);
                        String jobId = resultSet.getString(3);
                        int attempt = resultSet.getInt(4);
                        int status = resultSet.getInt(5);
                        String inputParams = resultSet.getString(6);
                        String outputParams = resultSet.getString(7);
                        String startTTime = resultSet.getString(8);
                        String endTTime = resultSet.getString(9);
                        Azkaban.Azkabanjobs azkabanjobs = azkaban.new Azkabanjobs(attempt, endTTime, flowId, inputParams, jobId, outputParams, projectId, startTTime, status);
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
                        int projectId = resultSet.getInt(1);
                        String flowId = resultSet.getString(2);
                        int status = resultSet.getInt(3);
                        String submitUser = resultSet.getString(4);
                        String submitTime = resultSet.getString(5);
                        String updateTime = resultSet.getString(6);
                        String startTTime = resultSet.getString(7);
                        String endTTime = resultSet.getString(8);
                        Azkaban.Azkabanflows azkabanflows = azkaban.new Azkabanflows(endTTime, flowId, projectId, startTTime, status, submitTime, submitUser, updateTime);
                        azkaban.putAzkabanflows(azkabanflows);
                    }
                }
                if ("projects".equals(entry.getKey())) {
                    String[] attr = entry.getValue().split(",");
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
                    String[] attr = entry.getValue().split(",");
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
}
