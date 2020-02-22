package com.cn.gp.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.cn.gp.flume.fields.CommonFields;
import com.cn.gp.flume.fields.MapFields;
import com.cn.gp.flume.service.DataCheck;
import com.cn.gp.project.datatype.DataTypeProperties;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 数据清洗, 自定义拦截器 </p>
 * @date 2020/2/21
 */
public class DataCleanInterceptor implements Interceptor {
    private static final Logger LOG = LoggerFactory.getLogger(DataCleanInterceptor.class);

    /**
     * 获取数据类型对应的字段, 对应的文件
     * 结构: [数据类型1 = [字段1，字段2...],
     * 数据类型2 = [字段1，字段2...]]
     */
    private static Map<String, ArrayList<String>> dataMap = DataTypeProperties.dataTypeMap;


    @Override
    public void initialize() {

    }

    /**
     * @return org.apache.flume.Event
     * @author GuYongtao
     * <p>单个消息拦截处理</p>
     * @date 2020/2/21
     */
    @Override
    public Event intercept(Event event) {
        if (event == null) {
            LOG.info("event为空...");
            return null;
        }
        SimpleEvent eventNew = new SimpleEvent();
        try {
            LOG.info("单个消息拦截器开始执行.");
            Map<String, String> map = parseEvent(event);
            if (map == null) {
                LOG.info("拦截处理后数据为空...");
                return null;
            }
            String lineJson = JSON.toJSONString(map);
            LOG.info("单个消息拦截器推送数据到 channel: " + lineJson);
            eventNew.setBody(lineJson.getBytes());
        } catch (Throwable t) {
            if (t instanceof Error) {
                throw (Error) t;
            }
            LOG.error("推送数据到channel失败", t);
        }


        return eventNew;
    }

    /**
     * @return java.util.List<org.apache.flume.Event>
     * @author GuYongtao
     * <p> 多条消息拦截处理 </p>
     * @date 2020/2/21
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> list = new ArrayList<>();
        for (Event event : events) {
            Event intercept = intercept(event);
            if (intercept != null) {
                list.add(intercept);
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    /**
     * @return java.util.Map<java.lang.String, java.lang.String>
     * @author GuYongtao
     * <p> 提取并校验消息 </p>
     * @date 2020/2/21
     */
    public static Map<String, String> parseEvent(Event event) {
        LOG.info("parseEvent");
        //  一条数据
        String line = new String(event.getBody(), StandardCharsets.UTF_8);
        String filename = event.getHeaders().get(MapFields.FILENAME);
        String absoluteFilename = event.getHeaders().get(MapFields.ABSOLUTE_FILENAME);

        // String 转Map, 并进行数据校验, 错误数据进入ES错误表
        LOG.info("hhhhhh");
        Map<String, String> stringStringMap = txtParse(line, filename, absoluteFilename);
        return stringStringMap;
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new DataCleanInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


    /**
     * @return java.util.Map<java.lang.String, java.lang.String>
     * @author GuYongtao
     * <p>数据长度校验，添加必要字段，长度不符合写入日志</p>
     * @date 2020/2/21
     */
    public static Map<String, String> txtParse(String line, String fileName, String absoluteFilename) {
        LOG.info("txtParse.");
        LOG.error("xxxxxxxxxxxxxxxxx.");
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

}
