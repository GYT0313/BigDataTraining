package com.cn.gp.flume.config;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> flume读取源文件 </p>
 * @date 2020/2/21
 */
public class ClusterConfigurationFileReader implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigurationFileReader.class);
    private static final long serialVersionUID = -1135978619846379424L;
    private final String DEFAULT_CONFIG_PATH = "flume/flume-config.properties";
    private static ClusterConfigurationFileReader reader;
    private static CompositeConfiguration configuration;

    public ClusterConfigurationFileReader() {
        LOG.info("加载配置文件: " + DEFAULT_CONFIG_PATH);
        configuration = new CompositeConfiguration();
        loadConfig(DEFAULT_CONFIG_PATH);
        LOG.info("加载配置文件成功.");
    }

    private void loadConfig(String path) {
        try {
            configuration.addConfiguration(new PropertiesConfiguration(path));
        } catch (ConfigurationException e) {
            LOG.error("加载配置文件失败: " + path, e);
        }
    }

    public static ClusterConfigurationFileReader getInstance() {
        if (reader == null) {
            synchronized (ClusterConfigurationFileReader.class) {
                if (reader == null) {
                    reader = new ClusterConfigurationFileReader();
                }
            }
        }
        return reader;
    }

    public CompositeConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * @return java.lang.String
     * @author GuYongtao
     * <p>获取key值</p>
     * @date 2020/2/21
     */
    public String getKey(String string) {
        return configuration.getString(string);
    }

    /**
     * @return int
     * @author GuYongtao
     * <p>获取int类型key值</p>
     * @date 2020/2/21
     */
    public int getKeyInt(String string) {
        return configuration.getInt(string);
    }

    /**
     * @return java.util.List<java.lang.String>
     * @author GuYongtao
     * <p>获取所有字段</p>
     * @date 2020/2/21
     */
    public List<String> getFields(String key) {
        List<String> list = new ArrayList<>();
        configuration.getList(key).forEach(x -> {
            list.add(x + "");
        });
        return list;
    }

    /**
     * @return int
     * @author GuYongtao
     * <p>获取DataType字段索引</p>
     * @date 2020/2/21
     */
    public int getIndex(String dataType, String field) {
        List<String> list = new ArrayList<>();
        configuration.getList(dataType).forEach(x -> {
            list.add(x + "");
        });
        int indexOf = list.indexOf(field);
        return indexOf + 1;
    }


}
