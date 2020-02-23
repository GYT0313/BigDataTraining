package com.cn.gp.kafka.config;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class KafkaConfigTest {
    @Test
    public void getKafkaProperties() {
        Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer",
                KafkaConfig.getInstance().getKafkaProperties().getProperty("key.serializer"));
    }
}
