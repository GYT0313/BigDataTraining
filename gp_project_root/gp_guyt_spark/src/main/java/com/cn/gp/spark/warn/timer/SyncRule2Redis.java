package com.cn.gp.spark.warn.timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 定时同步规则从MySQL到Redis </p>
 * @date 2020/2/23
 */
public class SyncRule2Redis extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRule2Redis.class);

    @Override
    public void run() {
        LOG.info("正从MySQL同步预警规则到Redis...");
        WarnHelper.syncRuleFromMysql2Redis();
    }
}
