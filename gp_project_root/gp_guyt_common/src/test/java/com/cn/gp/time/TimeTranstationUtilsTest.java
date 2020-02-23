package com.cn.gp.time;

import org.junit.Assert;
import org.junit.jupiter.api.Test;


class TimeTranstationUtilsTest {

    @Test
    void date2yyyyMMddHHmmss() {
        Assert.assertNotNull(TimeTranstationUtils.Date2yyyyMMddHHmmss());
    }

    @Test
    void testDate2yyyyMMdd() {
        Assert.assertEquals("2020-01-14", TimeTranstationUtils.Date2yyyyMMdd(1579011840000L));
    }

    @Test
    void testDate2yyyy_MM_dd() {
        System.out.println(TimeTranstationUtils.Date2yyyy_MM_dd());
    }
}