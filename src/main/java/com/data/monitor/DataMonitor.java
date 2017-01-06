package com.data.monitor;

import com.data.monitor.service.HDFSService;
import com.data.monitor.utils.ToolUtils;
import com.data.monitor.model.Email;
import com.data.monitor.model.QueryResult;
import com.data.monitor.service.AzkabanService;
import com.data.monitor.service.HiveSerivce;
import com.data.monitor.service.ServiceCenter;

import org.apache.log4j.Logger;

import java.util.Date;
import java.util.List;

/**
 * Created by Victor on 16-12-2.
 */
public class DataMonitor {
    private static int monitorDate = -1;//指定监测几天的数据
    private static final Logger logger = Logger.getLogger(DataMonitor.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("parameter numbers error ! , it must be one");
            System.exit(-1);
        } else {
            monitorDate = -Integer.parseInt(args[0]);
            if (-monitorDate < 0) {
                System.err.println("parameter 'monitorDate' error,it must be more than 0");
                System.exit(-1);
            }
        }
        run();
    }

    private static void run() throws Exception {
        if (!Config.initConfig()) {
            return;
        }
        Date beginTime = new Date();
        logger.info("monitor begin time: " + beginTime);
        String indexDay = ToolUtils.getDayBeforeFormat(monitorDate);
        String lastDay = ToolUtils.getDayBeforeFormat(-1);
        ServiceCenter azkabanService = new AzkabanService(indexDay, lastDay);
        QueryResult azkabanQuery = new QueryResult(azkabanService);
        MonitorController.getInstance().submit(azkabanService.getMonitorId(), azkabanQuery);

        List<String> list = ToolUtils.getDaysBeforeFormat(-monitorDate);
        ServiceCenter hiveService = new HiveSerivce(indexDay, lastDay);
        QueryResult hiveQuery = new QueryResult((String[]) list.toArray(new String[list.size()]), hiveService);
        MonitorController.getInstance().submit(hiveService.getMonitorId(), hiveQuery);

        ServiceCenter hdfsService = new HDFSService();
        QueryResult hdfsQuery = new QueryResult((String[]) list.toArray(new String[list.size()]), hdfsService);
        MonitorController.getInstance().submit(hdfsService.getMonitorId(), hdfsQuery);
        while ((MonitorController.getInstance().isActive() || MonitorController.getInstance().getWaitingQueueSize() > 0 || MonitorController.getInstance().getTotalSize() < 3)
                && System.currentTimeMillis() - beginTime.getTime() < Config.instance.getTimeOut()) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ResultView resultView = new ResultView();
        StringBuffer content = new StringBuffer("<html><meta charset='UTF-8'><meta http-equiv='Content-Type' content='text/html; charset=utf-8'/><body style='height:100%;margin:0;padding:0; list-style:none;'><div style='vertical-align: middle;margin: 0px auto;margin-bottom:20px;height:100%;'>")
                .append(resultView.generateView(list)).append("<h3>这是系统自动发送的邮件，请勿回复!</h3></div></body></html>");
        List<String> to = Config.instance.getSendTo();
        String subject = Config.instance.getSendSubject();
        Email email = new Email(to, subject, content.toString());
        email.send(Config.instance.getTmpPath());
        MonitorController.getInstance().shutdownPool();
        System.exit(0);
    }
}
