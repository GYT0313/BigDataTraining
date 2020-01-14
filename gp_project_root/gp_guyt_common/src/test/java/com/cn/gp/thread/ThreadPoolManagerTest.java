package com.cn.gp.thread;

import org.junit.Assert;
import org.junit.jupiter.api.Test;


class ThreadPoolManagerTest {

    @Test
    void getInstance() {
        Assert.assertNotNull(ThreadPoolManager.getInstance());
    }

}