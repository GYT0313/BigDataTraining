package com.cn.gp.data;

import java.text.DecimalFormat;
import java.util.Random;

public class RandomTude {
    public static String getRandomLongitude() {
        Random random = new Random(System.currentTimeMillis());
        DecimalFormat df = new DecimalFormat("#.000000");
        return String.valueOf(df.format(Fields.LONGITUDE + random.nextInt(15) * 0.01));
    }

    public static String getRandomLatitude() {
        Random random = new Random(System.currentTimeMillis());
        DecimalFormat df = new DecimalFormat("#.000000");
        return String.valueOf(df.format(Fields.LATITUDE + random.nextInt(15) * 0.01));
    }
}
