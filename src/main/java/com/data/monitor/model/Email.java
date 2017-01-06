package com.data.monitor.model;

import com.data.monitor.utils.FileUtil;
import com.data.monitor.ResultView;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.List;

/**
 * Created by Victor on 16-12-8.
 */
public class Email {
    private static final Logger logger = Logger.getLogger(Email.class);
    private List<String> to;
    private String subject;
    private String content;

    public Email(List<String> to, String subject, String content) {
        this.to = to;
        this.subject = subject;
        this.content = content;
    }

    public String listToString(List<String> toList) {
        if (toList == null) {
            return null;
        }
        StringBuffer result = new StringBuffer();
        boolean flag = false;
        for (String to : toList) {
            if (flag) {
                result.append(",");
            } else {
                flag = true;
            }
            result.append(to);
        }
        return result.toString();
    }

    public void callShell(String shellString) {
        String[] command = {"/bin/sh", "-c", shellString};
        try {
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                line += line;    //获取命令执行结果
            }
            input.close();
            int exitValue = process.waitFor();    //获取命令执行返回状态码
            if (0 != exitValue) {

                System.out.println("call shell failed. error code is :" + exitValue);
                System.out.println("Reason: " + line);
                logger.info("发送失败");
            } else {
                logger.info("发送成功");
            }
        } catch (Throwable e) {
            System.out.println("call shell failed. " + e);
        }
    }

    public void send(String tempPath) throws Exception {
        String mailMessage =
                "SUBJECT: {0}\n"    //邮件标题
                        + "TO: {1}\n"       //收件人
                        + "MIME-VERSION: 1.0\n"
                        + "Content-type: text/html;charset=UTF-8\n"
                        + "{2}\n";
        String receiver = listToString(this.to);
        String mail = MessageFormat.format(mailMessage, this.subject, receiver, this.content);
        FileUtil.writeTXTtoFile(mail, tempPath);
        String mailCommand = "cat " + tempPath + " | sendmail -t";
        callShell(mailCommand);
        if (ResultView.logs.size() > 0) {
            StringBuffer content = new StringBuffer("<html><meta charset='UTF-8'><meta http-equiv='Content-Type' content='text/html; charset=utf-8'/><body style='height:100%;margin:0;padding:0; list-style:none;'><div style='vertical-align: middle;margin: 0px auto;margin-bottom:20px;height:100%;'>");
            String attach = "logfile.txt";
            String dir = tempPath.substring(0, tempPath.lastIndexOf(File.separator));
            String su = "Azkaban失败任务log";
            content.append("<table border=1 width='98%' style='margin:auto;text-align:center'>").append("<tr>").append("<th colspan=").append(4).append(">").append(su).append("</th>").append("</tr>").append("<tr>")
                    .append("<td>Exec_Id</td><td>Content</td></tr>");

            for (Azkaban.Azkabanlogs azkabanlogs : ResultView.logs) {
                content.append("<tr>").append("<td>").append(azkabanlogs.getExecId()).append("</td>").append("<td>").append(azkabanlogs.getContent()).append("</td></tr>");
            }
            content.append("</table><br/>");
            content.append("<h3>这是系统自动发送的邮件，请勿回复!</h3></div></body></html>");
            mailMessage = MessageFormat.format(mailMessage, "AzkabanLog", receiver, content.toString());
            String path = dir + "/" + attach;
            FileUtil.writeTXTtoFile(mailMessage, path);
            mailCommand = "cat " + path + " | sendmail -t";
            callShell(mailCommand);
            FileUtil.deleteFile(path);
        }
        FileUtil.deleteFile(tempPath);
    }
}
