package com.cn.gp.spark.warn.service;

import com.cn.gp.spark.warn.dao.WarningMessageDao;
import com.cn.gp.spark.warn.domain.WarningMessage;
import com.cn.guyt.common.time.TimeTranstationUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/2/23
 */
public class BlackRuleWarning {
    private static final Logger LOG = LoggerFactory.getLogger(BlackRuleWarning.class);

    // 所有预警字段
    private static List<String> listWarnFields = new ArrayList<>();

    static {
        listWarnFields.add("phone");
        listWarnFields.add("mac");
    }


    public static void blackWarning(Map<String, Object> map, Jedis jedis) {
        LOG.info("开始预警校验...");
        listWarnFields.forEach(warnField -> {
            if (map.containsKey(warnField) && StringUtils.isNotBlank(map.get(warnField).toString())) {
                // 获取验证字段的值, 如phone
                String warFieldValue = map.get(warnField).toString();
                // 从redis中进行对比, redis中的key=phone:123456789, 所以进行拼接
                String key = warnField + ":" + warFieldValue;
                if (jedis.exists(key)) {
                    // 如果命中可以进行预警处理
                    beginWarning(jedis, key);
                }
            }
        });
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>预警处理</p>
     * @date 2020/2/23
     */
    private static void beginWarning(Jedis jedis, String key) {
        // 封装告警信息
        WarningMessage warningMessage = getWarningMessage(jedis, key);

        if (warningMessage != null) {
            // 将预警信息写入信息表
            WarningMessageDao.insertWarningMessageReturnId(warningMessage);
            // 手机警告
            if (warningMessage.getSendType().equals("2")) {
                WarningMessageSendUtil.messageWarn(warningMessage);
            }
        }

    }


    /**
     * @return com.cn.gp.spark.warn.domain.WarningMessage
     * @author GuYongtao
     * <p>封装告警信息</p>
     * @date 2020/2/23
     */
    private static WarningMessage getWarningMessage(Jedis jedis, String key) {
        String[] split = key.split(":");
        if (split.length >= 2) {
            WarningMessage warningMessage = new WarningMessage();

//            String clewType = split[0];
            String ruleContent = split[1];

            // 从redis中获取消息进行封装
            Map<String, String> valueMap = jedis.hgetAll(key);
            // 规则ID(那条规则命中)
            warningMessage.setAlarmRuleid(valueMap.get("id"));
            // 预警方式
            warningMessage.setSendType(valueMap.get("send_type"));
            // 预警信息接收手机
            warningMessage.setSendMobile(valueMap.get("send_mobile"));
            // 规则发布人
            warningMessage.setAccountid(valueMap.get("publisher"));
            // 默认预警发送方式
            warningMessage.setAlarmType("2");

            //预警内容 信息   时间  地点  人物
            //预警字段来进行设置  phone
            //我们有手机号


            //数据关联
            // 手机  MAC  身份证， 车牌  人脸。。URL 姓名
            // 全部设在推送消息里面
            StringBuffer warnContent = new StringBuffer();
            warnContent.append("【网络告警】：手机号：[" + ruleContent + "]在时间：[" +
                    TimeTranstationUtils.Date2yyyy_MM_dd_HH_mm_ss() + "]，出现在经纬：[]");
            warningMessage.setSenfInfo(warnContent.toString());
            return warningMessage;
        } else {
            return null;
        }
    }
}
