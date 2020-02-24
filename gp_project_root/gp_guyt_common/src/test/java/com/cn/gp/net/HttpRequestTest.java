package com.cn.gp.net;

import com.cn.gp.common.net.HttpRequest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

class HttpRequestTest {

    @Test
    void sendGet() {
        String s = HttpRequest.sendGet("https://www.baidu.com/s?", "中国");
        Assert.assertNotNull(s);
    }
}