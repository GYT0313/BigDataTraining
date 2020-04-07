package com.cn.gp.spark.warn.timer;

import com.cn.gp.spark.warn.dao.RuleDao;
import com.cn.gp.spark.warn.domain.RuleDomain;
import com.cn.gp.redis.client.JedisSingle;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 预警结果处理类 </p>
 * @date 2020/2/23
 */
public class WarnHelper {
    private static final Logger LOG = LoggerFactory.getLogger(WarnHelper.class);

    /**
     * @return void
     * @author GuYongtao
     * <p>同步MySQL的预警规则到redis</p>
     * @date 2020/2/23
     */
    public static void syncRuleFromMysql2Redis() {
        // 获取所有预警规则
        List<RuleDomain> ruleList = RuleDao.getRuleList();
        Jedis jedis = null;
        try {
            // 获取Redis客户端, 选择15号库
            jedis = JedisSingle.getJedis(15);
            for (int i = 0; i < ruleList.size(); i++) {
                RuleDomain ruleDomain = ruleList.get(i);
                String id = ruleDomain.getId() + "";
                String publisher = ruleDomain.getPublisher();
                String warnFieldName = ruleDomain.getWarnFieldName();
                String warnFieldValue = ruleDomain.getWarnFieldValue();
                String sendMobile = ruleDomain.getSendMobile();
                String sendType = ruleDomain.getSendType();

                // 拼接redis key值
                String redisKey = warnFieldName + ":" + warnFieldValue;
                // 通过redis hash结构
                jedis.hset(redisKey, "id", StringUtils.isNoneBlank(id) ? id : "");
                jedis.hset(redisKey, "publisher", StringUtils.isNoneBlank(publisher) ? publisher : "");
                jedis.hset(redisKey, "warnFieldName", StringUtils.isNoneBlank(warnFieldName) ? warnFieldName : "");
                jedis.hset(redisKey, "warnFieldValue", StringUtils.isNoneBlank(
                        warnFieldValue) ? warnFieldValue : "");
                jedis.hset(redisKey, "sendMobile", StringUtils.isNoneBlank(sendMobile) ? sendMobile : "");
                jedis.hset(redisKey, "sendType", StringUtils.isNoneBlank(sendType) ? sendType : "");
                LOG.info("同布预警规则成功...");
            }
        } catch (Exception e) {
            LOG.error("同步规则到redis 失败", e);
        } finally {
            JedisSingle.close(jedis);
        }
    }


    public static void main(String[] args) {
        WarnHelper.syncRuleFromMysql2Redis();
    }
}
