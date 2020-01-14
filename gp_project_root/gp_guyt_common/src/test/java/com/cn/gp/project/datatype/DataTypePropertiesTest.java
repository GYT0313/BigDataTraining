package com.cn.gp.project.datatype;


import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

class DataTypePropertiesTest {

    @Test
    public void test() {
        Map<String, ArrayList<String>> dataTypeMap = DataTypeProperties.dataTypeMap;
        Assert.assertNotNull(dataTypeMap.get("wechat"));
    }

}