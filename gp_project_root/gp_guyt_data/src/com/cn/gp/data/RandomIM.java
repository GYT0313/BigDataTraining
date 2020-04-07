package com.cn.gp.data;

import java.util.Random;

public class RandomIM {
    public static String getRandomIMEI() {
        Random random = new Random(System.currentTimeMillis());
        return String.valueOf(Fields.IMEI + random.nextInt(100));
    }

    public static String getRandomIMSI() {
        Random random = new Random(System.currentTimeMillis());
        return String.valueOf(Fields.IMSI + random.nextInt(100));
    }
}
