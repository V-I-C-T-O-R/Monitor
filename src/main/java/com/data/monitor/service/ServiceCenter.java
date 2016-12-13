package com.data.monitor.service;

import com.data.monitor.model.Result;
import com.data.monitor.utils.ToolUtils;

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
