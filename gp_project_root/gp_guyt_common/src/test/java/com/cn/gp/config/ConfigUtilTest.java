package com.cn.gp.config;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigUtilTest {

    @Test
    void getInstance() {
        Assert.assertNotNull(ConfigUtil.getInstance());
    }

    @Test
    void getProperties() {
        ConfigUtil configUtil = ConfigUtil.getInstance();
        Assert.assertNotNull(configUtil.getProperties("common/dataType.properties"));
    }
}