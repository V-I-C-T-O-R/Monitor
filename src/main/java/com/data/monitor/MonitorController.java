package com.data.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.data.monitor.model.QueryResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Victor on 16-12-5.
 */
public class MonitorController<T> {
    private volatile static MonitorController<QueryResult> jobJobsCollector;

    private ThreadPoolExecutor executor = null;
    private static Integer POOL_MIN_SIZE;
    private static Integer POOL_MAX_SIZE;
    private BlockingDeque<Runnable> waitQueue = new LinkedBlockingDeque<>();
    private Map<String, Future<?>> futureMap = new HashMap<>();
    private static ObjectMapper jsonMapper = new ObjectMapper();

    public static MonitorController<?> getInstance() {
        if (jobJobsCollector == null) {
            synchronized (MonitorController.class) {
                if (jobJobsCollector == null) {
                    jobJobsCollector = new MonitorController<>();
                }
            }
        }
        return jobJobsCollector;
    }

    private MonitorController() {
        POOL_MAX_SIZE = Config.instance.getPoolMax();
        POOL_MIN_SIZE = Config.instance.getPoolMin();
        if (POOL_MAX_SIZE == null) {
            POOL_MAX_SIZE = 20;
        }
        if (POOL_MIN_SIZE == null) {
            POOL_MIN_SIZE = 4;
        }
        executor = new ThreadPoolExecutor(POOL_MIN_SIZE, POOL_MAX_SIZE, 0L, TimeUnit.MILLISECONDS, waitQueue);
    }

    public void submit(String id, Callable<?> callable) {
        Future<?> f = executor.submit(callable);
        futureMap.put(id, f);
    }

    public void shutdownPool() {
        this.executor.shutdown();
        stopActive();
    }

    public void stopActive() {
        for (String key : futureMap.keySet()) {
            futureMap.get(key).cancel(true);
        }
    }

    public boolean isPoolFull() {
        int activeCount = this.executor.getActiveCount();
        return !(activeCount < POOL_MAX_SIZE);
    }

    public boolean isActive() {
        return this.executor.getActiveCount() > 0;
    }

    public long getTotalSize() {
        return futureMap.size();
    }

    public int getWaitingQueueSize() {
        return waitQueue.size();
    }

    public void clear() {
        shutdownPool();
        futureMap.clear();
        waitQueue.clear();
    }
}
