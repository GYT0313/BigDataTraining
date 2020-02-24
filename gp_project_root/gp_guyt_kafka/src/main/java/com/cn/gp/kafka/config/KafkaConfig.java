package com.cn.gp.kafka.config;

import com.cn.gp.common.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;


/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> Kafka配置, 解析器 </p>
 * @date 2020/2/22
 */
public class KafkaConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfig.class);

    private static final String DEFUALT_CONFIG_PATH = "kafka/kafka-server-config.properties";

    private volatile static KafkaConfig kafkaConfig = null;

    private Properties kafkaProperties;

    /**
     * 获取配置文件参数，并实例化
     *
     * @throws IOException
     */
    private KafkaConfig() throws IOException {
        try {
            kafkaProperties = ConfigUtil.getInstance().getProperties(DEFUALT_CONFIG_PATH);
        } catch (Exception e) {
            IOException ioException = new IOException();
            ioException.addSuppressed(e);
            throw ioException;
        }
    }

    /**
     * @return com.cn.gp.kafka.config.KafkaConfig
     * @author GuYongtao
     * <p>单例对象获取</p>
     * @date 2020/2/22
     */
    public static KafkaConfig getInstance() {
        if (kafkaConfig == null) {
            synchronized (KafkaConfig.class) {
                if (kafkaConfig == null) {
                    try {
                        kafkaConfig = new KafkaConfig();
                    } catch (IOException e) {
                        LOG.error("实例化KafkaConfig失败", e);
                    }
                }
            }
        }
        return kafkaConfig;
    }


    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

}
