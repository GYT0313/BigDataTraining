package com.cn.gp.config;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

class JsonReaderTest {

    @Test
    void readJson() {
        JsonReader jsonReader = new JsonReader();
        String json = jsonReader.readJson("es/mapping/base.json");
        Assert.assertNotNull(json);
    }
}