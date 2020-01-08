package com.cn.gp.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.ObjectInputFilter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author GuYongtao
 * @version 1.0.0
 * @className: ConfigUtil
 * @description: 读取配置文件
 * @date 2020/1/9
 */
public class ConfigUtil {

    private static Logger LOG = LoggerFactory.getLogger(ObjectInputFilter.Config.class);

    private static ConfigUtil configUtil;

    public static ConfigUtil getInstance() {
        if (configUtil == null) {
            configUtil = new ConfigUtil();
        }
        return configUtil;
    }

    public Properties getProperties(String path) {
        Properties properties = new Properties();
        try {
            LOG.info("开始加载配置文件: " + path);
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(path);
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            LOG.info("加载配置文件失败: " + path);
            LOG.error(null, e);
        }

        LOG.info("加载配置文件成功: " + path);
        LOG.info("文件内容: " + properties);
        return properties;
    }

}
