package com.sunteng.monitor.service;

import com.sunteng.monitor.dao.AzkabanDao;
import com.sunteng.monitor.model.Azkaban;
import com.sunteng.monitor.model.Result;

/**
 * Created by Victor on 16-12-5.
 */
public class AzkabanService extends ServiceCenter {
    private String startTime;
    private String endTime;

    public AzkabanService(String startTime, String endTime) {
        super();
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public Result getServiceStatus(String[] flags) throws Exception {
        Azkaban azkaban = AzkabanDao.getAzkabanList(this.startTime, this.endTime);
        azkaban.setType("azkaban");
        return azkaban;
    }
}
