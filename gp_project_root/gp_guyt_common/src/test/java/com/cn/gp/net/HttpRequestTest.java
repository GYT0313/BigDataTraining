package com.cn.gp.net;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpRequestTest {

    @Test
    void sendGet() {
        String s = HttpRequest.sendGet("https://www.baidu.com/s?", "中国");
        Assert.assertNotNull(s);
    }
}