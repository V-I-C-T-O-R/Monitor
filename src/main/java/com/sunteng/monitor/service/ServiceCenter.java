package com.sunteng.monitor.service;

import com.sunteng.monitor.model.Result;
import com.sunteng.monitor.utils.ToolUtils;

/**
 * Created by Victor on 16-12-5.
 */
public abstract class ServiceCenter {
    public String monitorId = ToolUtils.getUUID();

    public abstract Result getServiceStatus(String[] flags) throws Exception;

    public String getMonitorId() {
        return monitorId;
    }

}
