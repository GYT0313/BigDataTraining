package com.cn.gp.config;

import com.cn.gp.common.config.ConfigUtil;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

class ConfigUtilTest {

    @Test
    void getInstance() {
        Assert.assertNotNull(ConfigUtil.getInstance());
    }

    @Test
    void getProperties() {
        ConfigUtil configUtil = ConfigUtil.getInstance();
        Assert.assertNotNull(configUtil.getProperties("common/data-type.properties"));
    }
}