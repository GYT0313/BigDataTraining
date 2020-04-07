package com.cn.gp.data;

import java.util.Random;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/4/5
 */
public class RandomMAC {

    public static String getRandomMac() {
        Random random = new Random();
        String[] mac = {
                String.format("%02x", 0x52),
                String.format("%02x", 0x54),
                String.format("%02x", 0x00),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff))
        };
        return String.join(Fields.SEPARATOR_OF_MAC, mac);
    }
}
