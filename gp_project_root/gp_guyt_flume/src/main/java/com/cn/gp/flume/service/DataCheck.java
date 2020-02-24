package com.cn.gp.flume.service;

import com.alibaba.fastjson.JSON;
import com.cn.gp.flume.fields.CommonFields;
import com.cn.gp.flume.fields.ErrorMapFields;
import com.cn.gp.flume.fields.MapFields;
import com.cn.gp.common.net.HttpRequest;
import com.cn.gp.common.project.datatype.DataTypeProperties;
import com.cn.gp.common.time.TimeTranstationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 数据校验 </p>
 * @date 2020/2/21
 */
public class DataCheck {
    private final static Logger LOG = LoggerFactory.getLogger(DataCheck.class);

    /**
     * 获取数据类型对应的字段, 对应的文件
     * 结构: [数据类型1 = [字段1，字段2...],
     * 数据类型2 = [字段1，字段2...]]
     */
    private static Map<String, ArrayList<String>> dataMap = DataTypeProperties.dataTypeMap;

    /**
     * @return java.util.Map<java.lang.String, java.lang.String>
     * @author GuYongtao
     * <p>数据长度校验，添加必要字段，长度不符合写入日志</p>
     * @date 2020/2/21
     */
    public static Map<String, String> txtParse(String line, String fileName, String absoluteFilename) {
        Map<String, String> map = new HashMap<>(CommonFields.INITIAL_SIZE);
        String[] fileNames = fileName.split(CommonFields.FILE_NAME_SPLIT);
        // 第一位是数据类型
        String dataType = fileNames[0];
        if (dataMap.containsKey(dataType)) {
            List<String> fields = dataMap.get(dataType.toLowerCase());
            // 长度校验
            String[] lineSplits = line.split(CommonFields.LINE_SPLIT);
            if (fields.size() == lineSplits.length) {
                // 将数据的相关信息封装, 添加公共字段
                map.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
                map.put(MapFields.TABLE, dataType.toLowerCase());
                map.put(MapFields.RKSJ, (System.currentTimeMillis() / 1000) + "");
                map.put(MapFields.FILENAME, fileName);
                map.put(MapFields.ABSOLUTE_FILENAME, absoluteFilename);
                // 将消息对应封装
                for (int i = 0; i < lineSplits.length; i++) {
                    map.put(fields.get(i), lineSplits[i]);
                }
            } else {
                map = null;
                LOG.error("数据的字段长度不匹配, 需要长度: " + fields.size() + ", 当前长度：" + lineSplits.length);
            }
        } else {
            map = null;
            LOG.error("检索配置文件后, 无该类型数据, 文件名: " + fileName);
        }
        return map;
    }

    /**
     * @return java.util.Map<java.lang.String, java.lang.String>
     * @author GuYongtao
     * <p>数据长度校验，添加必要字段，长度不符合写入ES</p>
     * @date 2020/2/21
     */
    public static Map<String, String> txtParseAndValidation(String line, String fileName, String absoluteFileName) {
        Map<String, String> map = new HashMap<>(CommonFields.INITIAL_SIZE);
        Map<String, Object> errorMap = new HashMap<>(CommonFields.INITIAL_SIZE);
        // 文件名按"_"分割, 如: search_source1_uuid.txt
        // 分别是数据类型, 数据来源, UUID
        String[] fileNames = fileName.split(CommonFields.FILE_NAME_SPLIT);
        String dataType = fileNames[0];
//        String source = fileNames[1];

        if (dataMap.containsKey(dataType)) {
            // imei, imsi, longitude, latitude, phone_mac, device_mac, device_number, collect_time, username, phone,
            //      engine, content, search_url
            List<String> fields = dataMap.get(dataType.toLowerCase());
            // 长度校验
            String[] lineSplits = line.split(CommonFields.LINE_SPLIT);
            if (fields.size() == lineSplits.length) {
                // 将数据的相关信息封装, 添加公共字段
                map.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
                map.put(MapFields.TABLE, dataType.toLowerCase());
                map.put(MapFields.RKSJ, (System.currentTimeMillis() / 1000) + "");
                map.put(MapFields.FILENAME, fileName);
                map.put(MapFields.ABSOLUTE_FILENAME, absoluteFileName);
                // 将消息对应封装
                for (int i = 0; i < lineSplits.length; i++) {
                    map.put(fields.get(i), lineSplits[i]);
                }
                // 校验
                errorMap = DataValidation.dataValidation(map);
            } else {
                errorMap.put(ErrorMapFields.LENGTH, "数据分割长度不匹配, 需要长度: " + fields.size() +
                        ", 当前长度：" + lineSplits.length);
                errorMap.put(ErrorMapFields.LENGTH_ERROR, ErrorMapFields.LENGTH_ERROR_NUM);
                LOG.error("数据的字段长度不匹配, 需要长度: " + fields.size() + ", 当前长度：" + lineSplits.length);
                map = null;
            }

            // 校验数据是否存在错误
            if (null != errorMap && errorMap.size() > 0) {
                LOG.info("errorMap");
                if ("1".equals("1")) {
                    addErrorMapESByHttp(errorMap, map, fileName, absoluteFileName);
                }
                map = null;
            }

        } else {
            map = null;
            LOG.error("检索配置文件后, 无该类型数据, 文件名: " + fileName);
        }
        return map;
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>将错误信息以http方式写入es</p>
     * @date 2020/2/21
     */
    public static void addErrorMapESByHttp(Map<String, Object> errorMap, Map<String, String> map, String fileName,
                                           String absoluteFileName) {
        String errorType = fileName.split(CommonFields.FILE_NAME_SPLIT)[0];
        errorMap.put(MapFields.TABLE, errorType);
        errorMap.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
        errorMap.put(ErrorMapFields.RECORD, map);
        errorMap.put(ErrorMapFields.FILENAME, fileName);
        errorMap.put(ErrorMapFields.ABSOLUTE_FILENAME, absoluteFileName);
        errorMap.put(ErrorMapFields.RKSJ, TimeTranstationUtils.Date2yyyy_MM_dd_HH_mm_ss());
        String url = "http://192.168.1.111:9200/error_recourd/error_recourd/" + errorMap.get(MapFields.ID).toString();
        String json = JSON.toJSONString(errorMap);
        HttpRequest.sendPost(url, json);
    }


}
