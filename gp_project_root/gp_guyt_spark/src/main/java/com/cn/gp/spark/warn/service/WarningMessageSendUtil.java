package com.cn.gp.spark.warn.service;

import com.cn.gp.common.regex.Validation;
import com.cn.gp.spark.warn.domain.WarningMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarningMessageSendUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WarningMessageSendUtil.class);

    public static void messageWarn(WarningMessage warningMessage) {
        String[] mobiles = warningMessage.getSendMobile().split(",");
        for (String phone : mobiles) {
            if (Validation.isMobile(phone)) {
                LOG.info("向phone: " + phone + ", 发送告警消息: " + warningMessage);
                StringBuffer stringBuffer = new StringBuffer();
                String content = warningMessage.getSenfInfo();
                // TODO: 钉钉
            }
        }
    }
}
