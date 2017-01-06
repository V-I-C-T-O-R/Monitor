package com.data.monitor.dao;

import com.data.monitor.model.HDFS;
import com.data.monitor.Config;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Victor on 16-12-2.
 */
public class HDFSDao {
    private static final Logger logger = Logger.getLogger(HDFSDao.class);

    public static Map<String, List<Long>> getHDFSCapacity(String flag) {
        Map<String, List<String>> paths = null;
        Map<String, List<Long>> map = new HashMap<String, List<Long>>();
        try {
            paths = Config.instance.getHdfs();
            for (Map.Entry<String, List<String>> entry : paths.entrySet()) {
                List<String> indexs = entry.getValue();
                List<Long> couts = new ArrayList<Long>();
                for (String index : indexs) {
                    Path path = new Path(index + flag);
                    Long c = calCapacity(path);
                    couts.add(c);
                }
                map.put(entry.getKey(), couts);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getClass().getName() + ":" + e.getMessage());
        }
        return map;
    }

    public static HDFS getHDFSCapacitys(String[] flags) {
        HDFS hdfs = new HDFS();
        for (String flag : flags) {
            Map<String, List<Long>> map = getHDFSCapacity(flag);
            hdfs.getHdfsCount().put(flag, map);
        }
        return hdfs;
    }

    public static long calCapacity(Path path) throws IOException {
        FileSystem fs = Connections.getInstance().getHdfsConn();
        long total = 0L;
        if (fs.exists(path)) {
            FileStatus[] fileStatuses = fs.listStatus(path);
            for (FileStatus f : fileStatuses) {
                if (f.isFile())
                    total += f.getLen();
                else
                    total += calCapacity(f.getPath());
            }
        }
        return total;
    }
}
