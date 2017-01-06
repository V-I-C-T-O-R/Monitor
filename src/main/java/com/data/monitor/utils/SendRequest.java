package com.data.monitor.utils;

import com.data.monitor.Config;
import com.data.monitor.model.Message;

import net.sf.json.JSONObject;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by Victor on 16-12-20.
 */
public class SendRequest {
    private static final Logger logger = Logger.getLogger(SendRequest.class);

    public static String getSessionId() {
        JSONObject result = null;
        try {
            String loginUrl = "https://" + Config.instance.getAzkabanHostName() + ":" + String.valueOf(Config.instance.getAzkabanPort());
            String queryStr = "action=login&username=" + Config.instance.getAzkabanUsername() + "&password=" + Config.instance.getAzkabanPassword();
            result = AzkabanHttpsPost.post(loginUrl, queryStr, "POST");
            logger.info(result.toString());
            System.out.println("Azkaban login == " + result.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (String) result.get("session.id");
    }

    public static JSONObject sendRestartOrFeach(Map<String, String> params) {
        String exectUrl = "https://" + Config.instance.getAzkabanHostName() + ":" + String.valueOf(Config.instance.getAzkabanPort()) + "/executor";
        String executeParem = attemptRequest(params);
        JSONObject result = null;
        try {
            result = AzkabanHttpsPost.post(exectUrl, executeParem, "GET");
            logger.info(result.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private static String attemptRequest(Map<String, String> params) {
        StringBuffer sb = new StringBuffer();
        boolean flag = false;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (flag) {
                sb.append("&");
            } else {
                flag = true;
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }

    public static String sendWeChat(Message message) {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        List<NameValuePair> formparams = new ArrayList<NameValuePair>();
        formparams.add(new BasicNameValuePair("AgentId", message.getAgentId()));
        formparams.add(new BasicNameValuePair("Head", message.getHead()));
        formparams.add(new BasicNameValuePair("Safe", message.getSafe()));
        formparams.add(new BasicNameValuePair("Content", message.getContent()));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formparams, Consts.UTF_8);
        HttpPost httpPost = new HttpPost("http://"+Config.instance.getWeChatUrl());
        httpPost.setEntity(entity);
        CloseableHttpResponse response = null;
        try {
            //执行get请求并返回结果
            response = httpclient.execute(httpPost);
        } catch (IOException e) {
            e.printStackTrace();
        }
        HttpEntity en = response.getEntity();
        String result = null;
        try {
            result = EntityUtils.toString(entity);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
