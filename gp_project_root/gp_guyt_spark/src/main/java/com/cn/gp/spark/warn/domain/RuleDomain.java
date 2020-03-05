package com.cn.gp.spark.warn.domain;

import java.sql.Date;

/**
 * @author GuYongtao
 * <p>规则字段实体类</p>
 * @return
 * @date 2020/2/23
 */
public class RuleDomain {

    private int id;
    //预警字段
    private String warnFieldName;
    //预警内容
    private String warnFieldValue;
    //发布者
    private String publisher;
    //消息接收方式
    private String sendType;
    //接收手机号
    private String sendMobile;
    //接收邮箱
    private String sendMail;
    //接收钉钉
    private String sendDingDing;
    //创建时间
    private Date createTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getWarnFieldName() {
        return warnFieldName;
    }

    public void setWarnFieldName(String warnFieldName) {
        this.warnFieldName = warnFieldName;
    }

    public String getWarnFieldValue() {
        return warnFieldValue;
    }

    public void setWarnFieldValue(String warnFieldValue) {
        this.warnFieldValue = warnFieldValue;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
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

    public String getSendMail() {
        return sendMail;
    }

    public void setSendMail(String sendMail) {
        this.sendMail = sendMail;
    }

    public String getSendDingDing() {
        return sendDingDing;
    }

    public void setSendDingDing(String sendDingDing) {
        this.sendDingDing = sendDingDing;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "RuleDomain{" +
                "id=" + id +
                ", warnFieldName='" + warnFieldName + '\'' +
                ", warnFieldValue='" + warnFieldValue + '\'' +
                ", publisher='" + publisher + '\'' +
                ", sendType='" + sendType + '\'' +
                ", sendMobile='" + sendMobile + '\'' +
                ", sendMail='" + sendMail + '\'' +
                ", sendDingDing='" + sendDingDing + '\'' +
                ", createTime=" + createTime +
                '}';
    }
}
