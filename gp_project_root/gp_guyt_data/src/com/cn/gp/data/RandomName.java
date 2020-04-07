package com.cn.gp.data;

import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * @author GuYongtao 
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/4/5
 */
public class RandomName {

    public static String getChineseFamilyName() {

        String str = null;

        Random random = new Random(System.currentTimeMillis());


        int index = random.nextInt(Fields.FIRST_NAME.length - 1);
        //获得一个随机的姓氏
        str = Fields.FIRST_NAME[index];

        return str;

    }
    public static String getSecondName() {

        String str = null;

        Random random = new Random(System.currentTimeMillis());


        int index = random.nextInt(Fields.SECOND_NAME.length - 1);
        //获得一个随机的姓氏
        str = Fields.SECOND_NAME[index];

        return str;

    }

    //    getChineseGivenName方法具体实现如下

    public static String getChineseGivenName() {

        String str = null;

        int highPos, lowPos;

        Random random = new Random();
        //区码，0xA0打头，从第16区开始，即0xB0=11*16=176,16~55一级汉字，56~87二级汉字
        highPos = (176 + Math.abs(random.nextInt(71)));

        random = new Random();
        //位码，0xA0打头，范围第1~94列
        lowPos = 161 + Math.abs(random.nextInt(94));


        byte[] bArr = new byte[2];

        bArr[0] = (new Integer(highPos)).byteValue();

        bArr[1] = (new Integer(lowPos)).byteValue();

        try {
            //区位码组合成汉字
            str = new String(bArr, "GB2312");

        } catch (UnsupportedEncodingException e) {

            e.printStackTrace();

        }

        return str;

    }

    public static String getRandomName() {
        String name = "";

        String familyNameStr = "";

        Random random = new Random(System.currentTimeMillis());
        //随机标识
        Boolean flag = random.nextBoolean();

        familyNameStr = getChineseFamilyName();

        name = familyNameStr;

        if (flag) {
            //true,则名2个汉字
            name += getSecondName() + getSecondName();

        } else {
            //false,则名1个汉字
            name += getSecondName();

        }
        return name;
    }


}
