package com.data.monitor.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Victor on 16-12-2.
 */
public class Azkaban extends Result {
    private List<Azkabanjobs> azkabanjobsList = new ArrayList<Azkabanjobs>();
    private List<Azkabanflows> azkabanflowsList = new ArrayList<Azkabanflows>();
    private List<Azkabanprojects> azkabanprojectsList = new ArrayList<Azkabanprojects>();
    private List<Azkabanschedules> azkabanschedulesList = new ArrayList<Azkabanschedules>();

    public void putAzkabanjobs(Azkabanjobs azkabanjobs) {
        this.azkabanjobsList.add(azkabanjobs);
    }

    public void putAzkabanflows(Azkabanflows azkabanflows) {
        this.azkabanflowsList.add(azkabanflows);
    }

    public void putAzkabanprojects(Azkabanprojects azkabanprojects) {
        this.azkabanprojectsList.add(azkabanprojects);
    }

    public void putAzkabanschedules(Azkabanschedules azkabanschedules) {
        this.azkabanschedulesList.add(azkabanschedules);
    }

    public Azkabanjobs getAzkabanjob(int i) {
        return this.azkabanjobsList.get(i);
    }

    public List<Azkabanjobs> getAzkabanjob() {
        return this.azkabanjobsList;
    }

    public Azkabanflows getAzkabanflow(int i) {
        return this.azkabanflowsList.get(i);
    }

    public List<Azkabanflows> getAzkabanflow() {
        return this.azkabanflowsList;
    }

    public Azkabanprojects getAzkabanproject(int i) {
        return this.azkabanprojectsList.get(i);
    }

    public List<Azkabanprojects> getAzkabanproject() {
        return this.azkabanprojectsList;
    }

    public Azkabanschedules getAzkabanschedule(int i) {
        return this.azkabanschedulesList.get(i);
    }

    public List<Azkabanschedules> getAzkabanschedule() {
        return this.azkabanschedulesList;
    }

    public class Azkabanjobs {
        private int projectId;
        private String flowId;
        private String jobId;
        private int attempt;
        private int status;
        private String inputParams;
        private String outputParams;
        private String startTime;
        private String endTime;

        public Azkabanjobs(int attempt, String endTime, String flowId, String inputParams, String jobId, String outputParams, int projectId, String startTime, int status) {
            this.attempt = attempt;
            this.endTime = endTime;
            this.flowId = flowId;
            this.inputParams = inputParams;
            this.jobId = jobId;
            this.outputParams = outputParams;
            this.projectId = projectId;
            this.startTime = startTime;
            this.status = status;
        }

        public int getAttempt() {
            return attempt;
        }

        public String getEndTime() {
            return endTime;
        }

        public String getFlowId() {
            return flowId;
        }

        public String getInputParams() {
            return inputParams;
        }

        public String getJobId() {
            return jobId;
        }

        public String getOutputParams() {
            return outputParams;
        }

        public int getProjectId() {
            return projectId;
        }

        public String getStartTime() {
            return startTime;
        }

        public int getStatus() {
            return status;
        }
    }

    public class Azkabanflows {
        private int projectId;
        private String flowId;
        private int status;
        private String submitUser;
        private String submitTime;
        private String updateTime;
        private String startTime;
        private String endTime;

        public Azkabanflows(String endTime, String flowId, int projectId, String startTime, int status, String submitTime, String submitUser, String updateTime) {
            this.endTime = endTime;
            this.flowId = flowId;
            this.projectId = projectId;
            this.startTime = startTime;
            this.status = status;
            this.submitTime = submitTime;
            this.submitUser = submitUser;
            this.updateTime = updateTime;
        }

        public String getEndTime() {
            return endTime;
        }

        public String getFlowId() {
            return flowId;
        }

        public int getProjectId() {
            return projectId;
        }

        public String getStartTime() {
            return startTime;
        }

        public int getStatus() {
            return status;
        }

        public String getSubmitTime() {
            return submitTime;
        }

        public String getSubmitUser() {
            return submitUser;
        }

        public String getUpdateTime() {
            return updateTime;
        }
    }

    public class Azkabanprojects {
        private int projectId;
        private String projectName;
        private int active;
        private String modifiedTime;
        private String createTime;
        private String lastModifiedUser;

        public Azkabanprojects(int active, String createTime, String lastModifiedUser, String modifiedTime, String projectName, int projectId) {
            this.active = active;
            this.createTime = createTime;
            this.lastModifiedUser = lastModifiedUser;
            this.modifiedTime = modifiedTime;
            this.projectName = projectName;
            this.projectId = projectId;
        }

        public int getActive() {
            return active;
        }

        public String getCreateTime() {
            return createTime;
        }

        public String getLastModifiedUser() {
            return lastModifiedUser;
        }

        public String getModifiedTime() {
            return modifiedTime;
        }

        public String getProjectName() {
            return projectName;
        }

        public int getProjectId() {
            return projectId;
        }
    }

    public class Azkabanschedules {
        private int projectId;
        private String projectName;
        private String flowName;
        private String status;
        private String firstExecTime;
        private String period;
        private String lastModifyTime;
        private String nextExecTime;
        private String submitTime;
        private String submitUser;

        public Azkabanschedules(String firstExecTime, String flowName, String lastModifyTime, String nextExecTime, String period, int projectId, String projectName, String status, String submitTime, String submitUser) {
            this.firstExecTime = firstExecTime;
            this.flowName = flowName;
            this.lastModifyTime = lastModifyTime;
            this.nextExecTime = nextExecTime;
            this.period = period;
            this.projectId = projectId;
            this.projectName = projectName;
            this.status = status;
            this.submitTime = submitTime;
            this.submitUser = submitUser;
        }

        public String getFirstExecTime() {
            return firstExecTime;
        }

        public String getFlowName() {
            return flowName;
        }

        public String getLastModifyTime() {
            return lastModifyTime;
        }

        public String getNextExecTime() {
            return nextExecTime;
        }

        public String getPeriod() {
            return period;
        }

        public int getProjectId() {
            return projectId;
        }

        public String getProjectName() {
            return projectName;
        }

        public String getStatus() {
            return status;
        }

        public String getSubmitTime() {
            return submitTime;
        }

        public String getSubmitUser() {
            return submitUser;
        }
    }
}
