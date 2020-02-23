package com.cn.gp.flume.sink;

import com.cn.gp.flume.fields.CommonFields;
import com.cn.gp.kafka.producer.StringProducer;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 数据下沉到Kafka </p>
 * @date 2020/2/21
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
    private String[] kafkaTopics = null;

    @Override
    public Status process() {
        LOG.info("Flume - Sink开始执行...");
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            List<Event> events = takeBatchEvent(channel);
            if (events.isEmpty()) {
//                LOG.info("event is null");
                transaction.rollback();
                return Status.BACKOFF;
            }
            List<String> recordList = new LinkedList<>();
            events.forEach(event -> {
                // 获取事件内容
                recordList.add(new String(event.getBody()));
            });
            // 发送数据到Kafka
            try {
                StringProducer.producerBatch(kafkaTopics[0], recordList);
            } catch (Exception e) {
                LOG.error("下沉数据到Kafka失败", e);
                transaction.rollback();
            }

            transaction.commit();
            return Status.READY;
        } catch (ChannelException e) {
            LOG.error(null, e);
            transaction.rollback();
            return Status.BACKOFF;
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
    }

    /**
     * @return java.util.List<org.apache.flume.Event>
     * @author GuYongtao
     * <p>批量从channel取消息</p>
     * @date 2020/2/23
     */
    private List<Event> takeBatchEvent(Channel channel) {
        List<Event> eventsBatch = new LinkedList<>();
        for (int i=0; i < CommonFields.EVENT_BATCH_NUM; i++) {
            Event take = channel.take();
            if (take != null) {
                eventsBatch.add(take);
            } else {
                break;
            }
        }
        return eventsBatch;
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>配置读取</p>
     * @date 2020/2/21
     */
    @Override
    public void configure(Context context) {
        // kafkatopics参数在cdh的flume配置中, kafka_topic参数在flume-config.properties中
        if (context == null) {
            LOG.error("context为null");
        } else {
            kafkaTopics = context.getString("kafkatopics").split(",");
            LOG.info("数据下沉主题列表: " + context.getString("kafkatopics"));
        }
    }


}
