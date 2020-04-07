package com.cn.gp.data;

import java.util.Random;

public class RandomContent {
    public static String getRandomContent() {
        Random random = new Random(System.currentTimeMillis());
        int num1 = random.nextInt(Fields.SEARCH_CONTENT.length - 1);
        int num2 = random.nextInt(Fields.SEARCH_CONTENT[0].length - 1);
        return Fields.SEARCH_CONTENT[num1][num2];
    }
}
