package com.sunteng.monitor.model;

/**
 * Created by Victor on 16-12-22.
 */
public class Message {
    private String agentId;
    private String head;
    private String content;
    private String safe;

    public Message(String agentId, String content, String head, String safe) {
        this.agentId = agentId;
        this.content = content;
        this.head = head;
        this.safe = safe;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getHead() {
        return head;
    }

    public void setHead(String head) {
        this.head = head;
    }

    public String getSafe() {
        return safe;
    }

    public void setSafe(String safe) {
        this.safe = safe;
    }
}
