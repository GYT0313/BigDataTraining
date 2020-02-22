package com.cn.gp.flume.sink;

import avro.shaded.com.google.common.base.Throwables;
import com.cn.gp.kafka.producer.StringProducer;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private List<String> listKeyMessage = null;
    private Long processTimestamp = System.currentTimeMillis();


    @Override
    public Status process() throws EventDeliveryException {
        LOG.info("Flume - Sink开始执行...");
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            if (event == null) {
                LOG.info("event is null");
                transaction.rollback();
                return Status.BACKOFF;
            }
            // 获取事件内容
            String record = new String(event.getBody());
            // 发送数据到Kafka
            try {
                StringProducer.producer(kafkaTopics[0], record);
/*                listKeyedMessage.add(recourd);
                if(listKeyedMessage.size()>20){
                    logger.info("数据大与10000,推送数据到kafka");
                    StringProducer stringProducer = new StringProducer();
                    stringProducer.producer(kafkatopics[0],listKeyedMessage);
                    logger.info("数据大与10000,推送数据到kafka成功");
                }else if(System.currentTimeMillis()-proTimestamp>=60*1000){

                    logger.info("时间间隔大与60,推送数据到kafka");
                    StringProducer stringProducer = new StringProducer();
                    stringProducer.producer(kafkatopics[0],listKeyedMessage);
                    logger.info("时间间隔大与60,推送数据到kafka成功"+listKeyedMessage.size());
                }*/
            } catch (Exception e) {
                LOG.error("下沉数据到Kafka失败", e);
                throw Throwables.propagate(e);

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
            LOG.info("获取Kafka Topic配置: " + context.getString("kafkatopics"));
            listKeyMessage = new ArrayList<>();
        }
    }


}
