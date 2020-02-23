package com.cn.gp.spark.common.convert;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/2/23
 */
public class BaseDataConvert {
    private static final Logger LOG = LoggerFactory.getLogger(BaseDataConvert.class);

    /**
     * @return java.util.HashMap<java.lang.String, java.lang.Object>
     * @author GuYongtao
     * <p>字符串转长整形</p>
     * @date 2020/2/23
     */
    public static HashMap<String, Object> mapString2Long(Map<String, String> map, String key,
                                                         HashMap<String, Object> objectHashMap) {
        String logOutTime = (String) map.get(key);
        if (StringUtils.isNotBlank(logOutTime)) {
            objectHashMap.put(key, Long.valueOf(logOutTime));
        } else {
            objectHashMap.put(key, 0L);
        }
        return objectHashMap;
    }

    /**
     * @return java.util.HashMap<java.lang.String, java.lang.Object>
     * @author GuYongtao
     * <p>字符串转双精度浮点数</p>
     * @date 2020/2/23
     */
    public static HashMap<String, Object> mapString2Double(Map<String, String> map, String key,
                                                           HashMap<String, Object> objectHashMap) {
        String logOutTime = (String) map.get(key);
        if (StringUtils.isNotBlank(logOutTime)) {
            objectHashMap.put(key, Double.valueOf(logOutTime));
        } else {
            objectHashMap.put(key, 0L);
        }
        return objectHashMap;
    }

    public static HashMap<String, Object> mapString2String(Map<String, String> map, String key,
                                                           HashMap<String, Object> objectHashMap) {
        String logOutTime = map.get(key);
        if (StringUtils.isNotBlank(logOutTime)) {
            objectHashMap.put(key, logOutTime);
        } else {
            objectHashMap.put(key, "");
        }
        return objectHashMap;
    }


}
