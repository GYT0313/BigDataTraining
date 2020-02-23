package com.cn.gp.flume.config;

import org.junit.Assert;
import org.junit.Test;

public class ClusterConfigurationFileReaderTest {


    @Test
    public void getConfiguration() {
        ClusterConfigurationFileReader reader = new ClusterConfigurationFileReader();
        Assert.assertEquals("gp_1", reader.getConfiguration().getString("kafka_topic"));
    }
}
