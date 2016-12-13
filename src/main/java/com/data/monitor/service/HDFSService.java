package com.data.monitor.service;

import com.data.monitor.dao.HDFSDao;
import com.data.monitor.model.HDFS;
import com.data.monitor.model.Result;

/**
 * Created by Victor on 16-12-5.
 */
public class HDFSService extends ServiceCenter {
    @Override
    public Result getServiceStatus(String[] flags) throws Exception {
        HDFS hdfs = HDFSDao.getHDFSCapacitys(flags);
        hdfs.setType("hdfs");
        return hdfs;
    }
}
