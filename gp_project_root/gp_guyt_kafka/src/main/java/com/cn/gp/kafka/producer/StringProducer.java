package com.cn.gp.kafka.producer;

import com.cn.gp.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 生产者 </p>
 * @date 2020/2/22
 */
public class StringProducer {
    private static final Logger LOG = LoggerFactory.getLogger(StringProducer.class);

    /**
     * 异步发送批量条消息
     *
     * @param topic
     * @param recordList
     */
    public static void producerBatch(String topic, List<String> recordList) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                KafkaConfig.getInstance().getKafkaProperties());
        AtomicInteger num = new AtomicInteger();
        // 同步发送消息
        try {
            recordList.forEach(record -> {
                try {
                    kafkaProducer.send(new ProducerRecord<>(topic, "guyt", record)).get();
                    num.addAndGet(1);
                } catch (Exception e) {
                    LOG.error("Kafka-producer发送消息失败: " + record, e);
                }
            });
        } finally {
            LOG.info("成功下沉数据条数: " + num);
            kafkaProducer.close();
        }
    }

    /**
     * @author GuYongtao
     * @version 1.0.0
     * <p> 回调类 </p>
     * @date 2020/2/22
     */
    private class DemoProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            // TODO Auto-generated method stub
            if (e != null) {
                // 如果消息发送失败，打印异常
                LOG.error("Kafka-producer发送消息失败: " + recordMetadata.toString(), e);
            } else {
                LOG.info("Kafka-producer发送消息成功.");
            }
        }
    }

}
