package com.cn.gp.data;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/4/5
 */
public class RandomSearch {
    public static String getRandomEngine(int index) {
        return Fields.SEARCH_ENGINE[index];
    }

    public static String getRandomSearchURL(int index) {
        return Fields.SEARCH_URL[index];
    }
}
