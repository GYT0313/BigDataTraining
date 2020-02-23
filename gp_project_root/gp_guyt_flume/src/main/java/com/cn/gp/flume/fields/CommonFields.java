package com.cn.gp.flume.fields;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 常量字段 </p>
 * @date 2020/2/21
 */
public class CommonFields {
    public static final int INITIAL_SIZE = 16;
    public static final String FILE_NAME_SPLIT = "_";
    public static final String LINE_SPLIT = "\t";
    public static final String MIDDLE_LINE = "-";

    // 每次从channel取20条消息
    public static final int EVENT_BATCH_NUM = 100;
}
