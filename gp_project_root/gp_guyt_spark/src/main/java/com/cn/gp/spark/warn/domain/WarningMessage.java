package com.cn.gp.spark.warn.domain;

import java.sql.Date;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 警告消息 </p>
 * @date 2020/2/23
 */
public class WarningMessage {
    //主键id
    private String id;
    //规则id
    private String alarmRuleid;
    //告警类型
    private String alarmType;
    //发送方式
    private String sendType;
    //发送至手机
    private String sendMobile;
    //发送至邮箱
    private String sendEmail;
    //发送状态
    private String sendStatus;
    //发送内容
    private String senfInfo;
    //命中时间
    private Date hitTime;
    //入库时间
    private Date checkinTime;
    //是否已读
    private String isRead;
    //已读用户
    private String readAccounts;
    private String alarmaccounts;
    private String accountid;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAlarmRuleid() {
        return alarmRuleid;
    }

    public void setAlarmRuleid(String alarmRuleid) {
        this.alarmRuleid = alarmRuleid;
    }

    public String getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(String alarmType) {
        this.alarmType = alarmType;
    }

    public String getSendType() {
        return sendType;
    }

    public void setSendType(String sendType) {
        this.sendType = sendType;
    }

    public String getSendMobile() {
        return sendMobile;
    }

    public void setSendMobile(String sendMobile) {
        this.sendMobile = sendMobile;
    }

    public String getSendEmail() {
        return sendEmail;
    }

    public void setSendEmail(String sendEmail) {
        this.sendEmail = sendEmail;
    }

    public String getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(String sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getSenfInfo() {
        return senfInfo;
    }

    public void setSenfInfo(String senfInfo) {
        this.senfInfo = senfInfo;
    }

    public Date getHitTime() {
        return hitTime;
    }

    public void setHitTime(Date hitTime) {
        this.hitTime = hitTime;
    }

    public Date getCheckinTime() {
        return checkinTime;
    }

    public void setCheckinTime(Date checkinTime) {
        this.checkinTime = checkinTime;
    }

    public String getIsRead() {
        return isRead;
    }

    public void setIsRead(String isRead) {
        this.isRead = isRead;
    }

    public String getReadAccounts() {
        return readAccounts;
    }

    public void setReadAccounts(String readAccounts) {
        this.readAccounts = readAccounts;
    }

    public String getAlarmaccounts() {
        return alarmaccounts;
    }

    public void setAlarmaccounts(String alarmaccounts) {
        this.alarmaccounts = alarmaccounts;
    }

    public String getAccountid() {
        return accountid;
    }

    public void setAccountid(String accountid) {
        this.accountid = accountid;
    }

    @Override
    public String toString() {
        return "WarningMessage{" +
                "id='" + id + '\'' +
                ", alarmRuleid='" + alarmRuleid + '\'' +
                ", alarmType='" + alarmType + '\'' +
                ", sendType='" + sendType + '\'' +
                ", sendMobile='" + sendMobile + '\'' +
                ", sendEmail='" + sendEmail + '\'' +
                ", sendStatus='" + sendStatus + '\'' +
                ", senfInfo='" + senfInfo + '\'' +
                ", hitTime=" + hitTime +
                ", checkinTime=" + checkinTime +
                ", isRead='" + isRead + '\'' +
                ", readAccounts='" + readAccounts + '\'' +
                ", alarmaccounts='" + alarmaccounts + '\'' +
                ", accountid='" + accountid + '\'' +
                '}';
    }
}
