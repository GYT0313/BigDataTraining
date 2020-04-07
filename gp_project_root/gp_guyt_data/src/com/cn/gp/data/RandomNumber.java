package com.cn.gp.data;

import java.util.Random;

/**
 * @author GuYongtao 
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/4/5
 */
public class RandomNumber {

    public static String getRandomPhoneNumber() {
        long phone = 15802801000L;
        Random random = new Random(System.currentTimeMillis());
        return String.valueOf(phone + random.nextInt(20));
    }

    public static String getRandomDeviceNumber() {
        long device = 1000L;
        Random random = new Random(System.currentTimeMillis());
        return String.valueOf(device + random.nextInt(100));
    }

}
