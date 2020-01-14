package com.cn.gp.project.datatype;

import com.cn.gp.config.ConfigUtil;
import com.cn.gp.fields.CommonFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 获取数据类型 </p>
 * @date 2020/1/14
 */
public class DataTypeProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTypeProperties.class);

    public static Map<String, ArrayList<String>> dataTypeMap = null;

    static {
        Properties properties = ConfigUtil.getInstance().getProperties(CommonFields.DATA_TYPE_PATH);
        dataTypeMap = new HashMap<>();
        Set<Object> keys = properties.keySet();
        keys.forEach(key -> {
            String[] split = properties.getProperty(key.toString()).split(",");
            dataTypeMap.put(key.toString(), new ArrayList<>(Arrays.asList(split)));
        });
    }
}
