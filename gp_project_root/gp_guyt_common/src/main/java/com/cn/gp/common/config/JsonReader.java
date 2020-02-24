package com.cn.gp.common.config;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>
 * JSON文件读取
 * </p>
 * @date 2020/1/9
 */
public class JsonReader {
    private static Logger LOG = LoggerFactory.getLogger(JsonReader.class);

    public static String readJson(String jsonPath) {
        JsonReader jsonReader = new JsonReader();
        return jsonReader.getJson(jsonPath);
    }

    /**
     * @param [jsonPath]
     * @return java.lang.String
     * @author GuYongtao
     * <p>  </p>
     * @date 2020/1/9
     */
    private String getJson(String jsonPath) {
        String jsonStr = "";
        try {
            String path = getClass().getClassLoader().getResource(jsonPath).toString();
            if (path.contains(":")) {
                path = path.replace("file:/", "");
            }
            jsonStr = FileUtils.readFileToString(new File(path), "UTF-8");
            LOG.info("读取JSON成功: " + path);
        } catch (IOException e) {
            LOG.error("读取JSON文件失败: " + jsonPath, e);
        }
        return jsonStr;
    }
}
