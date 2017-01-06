package com.data.monitor.service;

import com.data.monitor.dao.HiveDao;
import com.data.monitor.model.Hive;
import com.data.monitor.model.Result;

/**
 * Created by Victor on 16-12-5.
 */
public class HiveSerivce extends ServiceCenter {
    private String startTime;
    private String endTime;

    public HiveSerivce(String startTime, String endTime) {
        super();
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public Result getServiceStatus(String[] flags) throws Exception {
        Hive hive = HiveDao.getHiveMap(this.startTime, this.endTime, flags);
        hive.setType("hive");
        return hive;
    }
}
