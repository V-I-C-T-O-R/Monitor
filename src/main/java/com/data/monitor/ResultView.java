package com.data.monitor;

import com.data.monitor.model.Azkaban;
import com.data.monitor.model.HDFS;
import com.data.monitor.model.Hive;
import com.data.monitor.model.QueryResult;
import com.data.monitor.utils.ToolUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Victor on 16-12-6.
 */
public class ResultView {
    public String generateView(List<String> list) {
        // 核对大小、数量
        StringBuffer sb = new StringBuffer();
        sb.append(getHDFSStatus(list)).append(getHiveStatus(list));
        for (String s : getAzkabanStatus()) {
            sb.append(s);
        }
        return sb.toString();
    }

    private String getHDFSStatus(List<String> list) {
        HDFS hdfs = (HDFS) QueryResult.results.get("hdfs");
        StringBuffer sb = new StringBuffer();
        Map<String, List<Long>> oldCounts = new HashMap<String, List<Long>>();
        Map<String, List<Long>> newCounts = new HashMap<String, List<Long>>();
        List<String> tableName = new ArrayList<String>();
        StringBuffer subject = new StringBuffer("HDFS--").append(list.size()).append("天历史数据量对比");
        sb.append("<table border=1 width='98%' style='margin:auto;text-align:center'>").append("<tr>").append("<th colspan=").append(list.size() + 2).append(">").append(subject.toString()).append("</th>").append("</tr>").append("<tr><td></td><td></td>");
        boolean mark = false;
        for (int i = 0; i < list.size(); i++) {
            sb.append("<td>").append(list.get(i)).append("</td>");
            for (Map.Entry<String, List<Long>> map : hdfs.getHdfsCount().get(list.get(i)).entrySet()) {
                if (oldCounts.get(map.getKey()) == null) {
                    List<Long> l = new ArrayList<Long>();
                    l.add(map.getValue().get(0));
                    oldCounts.put(map.getKey(), l);
                } else {
                    oldCounts.get(map.getKey()).add(map.getValue().get(0));
                }
                if (newCounts.get(map.getKey()) == null) {
                    List<Long> l = new ArrayList<Long>();
                    l.add(map.getValue().get(1));
                    newCounts.put(map.getKey(), l);
                } else {
                    newCounts.get(map.getKey()).add(map.getValue().get(1));
                }
                if (!mark) {
                    tableName.add(map.getKey());
                }
            }
            mark = true;
        }
        sb.append("</tr>");
        for (String name : tableName) {
            sb.append("<tr><td>").append(name).append("</td><td>元数据</td>");
            for (int i = 0; i < list.size(); i++) {

                sb.append("<td>").append(oldCounts.get(name).get(i)).append("</td>");
            }
            sb.append("</tr>");
            sb.append("<tr><td>").append(name).append("</td><td>新数据</td>");
            for (int i = 0; i < list.size(); i++) {
                sb.append("<td>").append(newCounts.get(name).get(i)).append("</td>");
            }
            sb.append("</tr>");

        }
        sb.append("</table></br>");
        return sb.toString();
    }

    private String getHiveStatus(List<String> list) {
        Hive hive = (Hive) QueryResult.results.get("hive");
        StringBuffer sb = new StringBuffer();
        StringBuffer subject = new StringBuffer("Hive--").append(list.size()).append("天总数据量对比");
        sb.append("<table border=1 width='98%' style='margin:auto;text-align:center'>").append("<tr>").append("<th colspan=").append(list.size() + 2).append(">").append(subject.toString()).append("</th>").append("</tr>").append("<tr><td>表名</td><td>数据量(单位/条)</td>");
        for (int i = 0; i < list.size(); i++) {
            sb.append("<td>partition-").append(list.get(i)).append("</td>");
        }
        sb.append("</tr>");
        for (Map.Entry<String, Long> entry : hive.getCountRecords().entrySet()) {
            sb.append("<tr>").append("<td>").append(entry.getKey()).append("</td>").append("<td>").append(entry.getValue()).append("</td>");
            for (int i = 0; i < list.size(); i++) {
                boolean b = hive.getValidatPartition().get(entry.getKey()).get(list.get(i));
                sb.append("<td>").append(b).append("</td>");
            }
            sb.append("</tr>");
        }
        sb.append("</table></br>");
        return sb.toString();
    }

    private List<String> getAzkabanStatus() {
        Azkaban azkaban = (Azkaban) QueryResult.results.get("azkaban");
        Map<Integer, String> names = new HashMap<Integer, String>();
        List<String> status = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        String subject = "Azkaban现有任务列表";
        sb.append("<table border=1 width='98%' style='margin:auto;text-align:center'>").append("<tr>").append("<th colspan=").append(6).append(">").append(subject).append("</th>").append("</tr>").append("<tr>")
                .append("<td>任务ID</td><td>任务名称</td><td>创建时间</td><td>最后修改人</td><td>最后修改时间</td><td>是否活跃</td></tr>");
        for (Azkaban.Azkabanprojects azkabanprojects : azkaban.getAzkabanproject()) {
            String stat = null;
            if (azkabanprojects.getActive() == 0) {
                stat = "停止";
            } else if (azkabanprojects.getActive() == 1) {
                stat = "运行";
            } else if (azkabanprojects.getActive() == 2) {
                stat = "暂停";
            } else {
                stat = "未知";
            }
            sb.append("<tr>").append("<td>").append(azkabanprojects.getProjectId()).append("</td>")
                    .append("<td>").append(azkabanprojects.getProjectName()).append("</td>")
                    .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanprojects.getCreateTime()))).append("</td>")
                    .append("<td>").append(azkabanprojects.getLastModifiedUser()).append("</td>")
                    .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanprojects.getModifiedTime()))).append("</td>")
                    .append("<td>").append(stat).append("</td>");
            sb.append("</tr>");
            names.put(azkabanprojects.getProjectId(), azkabanprojects.getProjectName());
        }
        sb.append("</table></br>");
        status.add(sb.toString());

        sb = new StringBuffer();
        subject = "Azkaban定时任务列表";
        sb.append("<table border=1 width='98%' style='margin:auto;text-align:center'>").append("<tr>").append("<th colspan=").append(10).append(">").append(subject).append("</th>").append("</tr>").append("<tr>")
                .append("<td>任务ID</td><td>任务名称</td><td>Flow名称</td><td>当前状态</td><td>首次执行时间</td><td>执行周期</td><td>下次执行时间</td><td>最后修改时间</td><td>创建者</td><td>创建时间</td></tr>");
        for (Azkaban.Azkabanschedules azkabanschedules : azkaban.getAzkabanschedule()) {
            sb.append("<tr>").append("<td>").append(azkabanschedules.getProjectId()).append("</td>")
                    .append("<td>").append(azkabanschedules.getProjectName()).append("</td>")
                    .append("<td>").append(azkabanschedules.getFlowName()).append("</td>")
                    .append("<td>").append(azkabanschedules.getStatus()).append("</td>")
                    .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanschedules.getFirstExecTime()))).append("</td>")
                    .append("<td>").append(azkabanschedules.getPeriod()).append("</td>")
                    .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanschedules.getNextExecTime()))).append("</td>")
                    .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanschedules.getLastModifyTime()))).append("</td>")
                    .append("<td>").append(azkabanschedules.getSubmitUser()).append("</td>")
                    .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanschedules.getSubmitTime()))).append("</td>");
            sb.append("</tr>");
        }
        sb.append("</table></br>");
        status.add(sb.toString());

        sb = new StringBuffer();
        subject = "Azkaban执行失败任务列表";
        sb.append("<table border=1 width='98%' style='margin:auto;text-align:center'>").append("<tr>").append("<th colspan=").append(10).append(">").append(subject).append("</th>").append("</tr>").append("<tr>")
                .append("<td>任务ID</td><td>Job_ID</td><td>当前状态</td><td>输入参数</td><td>输出参数</td><td>开始时间</td><td>结束时间</td><td>提交人</td><td>提交时间</td><td>更新时间</td></tr>");
        for (Azkaban.Azkabanjobs azkabanjobs : azkaban.getAzkabanjob()) {
            if (azkabanjobs.getStatus() == 0) {   //暂时失败定为0
                sb.append("<tr>").append("<td>").append(names.get(azkabanjobs.getProjectId())).append("</td>")
                        .append("<td>").append(azkabanjobs.getJobId()).append("</td>")
                        .append("<td>").append(azkabanjobs.getStatus()).append("</td>")
                        .append("<td>").append(azkabanjobs.getInputParams()).append("</td>")
                        .append("<td>").append(azkabanjobs.getOutputParams()).append("</td>")
                        .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanjobs.getStartTime()))).append("</td>")
                        .append("<td>").append(ToolUtils.MF.format(Long.valueOf((azkabanjobs.getEndTime())))).append("</td>");
                for (Azkaban.Azkabanflows azkabanflows : azkaban.getAzkabanflow()) {
                    if (azkabanjobs.getFlowId().equals(azkabanflows.getFlowId())) {
                        sb.append("<td>").append(azkabanflows.getSubmitUser()).append("</td>")
                                .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanflows.getSubmitTime()))).append("</td>")
                                .append("<td>").append(ToolUtils.MF.format(Long.valueOf(azkabanflows.getUpdateTime()))).append("</td>");
                        break;
                    }
                }
                sb.append("</tr>");
            }
        }
        sb.append("</table></br>");
        status.add(sb.toString());
        return status;
    }
}
