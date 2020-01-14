package com.cn.gp.regex;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ValidationTest {

    @Test
    void isEmail() {
        boolean email = Validation.isEmail("guyongtao.me@gmail.com");
        Assert.assertTrue(email);
    }

    @Test
    void isPhone() {
        boolean phone = Validation.isPhone("123s");
        Assert.assertFalse(phone);
    }
}