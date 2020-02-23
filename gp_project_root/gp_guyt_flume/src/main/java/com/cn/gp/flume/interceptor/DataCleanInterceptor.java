package com.cn.gp.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.cn.gp.flume.fields.MapFields;
import com.cn.gp.flume.service.DataCheck;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 数据清洗, 自定义拦截器 </p>
 * @date 2020/2/21
 */
public class DataCleanInterceptor implements Interceptor {
    private static final Logger LOG = LoggerFactory.getLogger(DataCleanInterceptor.class);
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
            Map<String, String> map = parseEvent(event);
            if (map == null) {
                return null;
            }
            String lineJson = JSON.toJSONString(map);
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

        //  一条数据
        String line = new String(event.getBody(), StandardCharsets.UTF_8);
        String filename = event.getHeaders().get(MapFields.FILENAME);
        String absoluteFilename = event.getHeaders().get(MapFields.ABSOLUTE_FILENAME);

        // String 转Map, 并进行数据校验, 错误数据进入ES错误表
        return DataCheck.txtParseAndValidation(line, filename, absoluteFilename);
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

}
