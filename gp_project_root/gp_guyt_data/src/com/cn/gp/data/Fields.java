package com.cn.gp.data;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/4/5
 */
public class Fields {

    /**
     * 随机参数
     */


    /**
     * imei 15位，后三位随机生成
     * imsi 15位，后三位随机生成
     */
    public final static long IMEI = 868706048938233L;
    public final static long IMSI = 460028028330233L;

    /**
     * 经纬小数点保留10位
     */
    public final static double LONGITUDE = 30.657442;
    public final static double LATITUDE = 35.065764;

    /**
     * phone MAC
     * device MAC
     */
    public final static String SEPARATOR_OF_MAC = "-";


    /**
     * 598 百家姓
     */
    public final static String[] FIRST_NAME = {"吴","郑","王","冯","韩","杨","朱","秦","尤","许"};
    public final static String[] SECOND_NAME = {"赵","钱","郑","王","冯","陈","褚","卫","朱","秦"};



    /**
     * 搜索引擎
     */
    public final static String[] SEARCH_ENGINE = {"百度", "谷歌", "搜狗", "必应", "360"};
    public final static String[] SEARCH_URL = {"https://www.baidu.com/s?wd=", "https://www.google.com.hk/search?q=",
            "https://www.sogou.com/web?query=", "https://cn.bing.com/search?q=", "https://www.so.com/s?q="};


    /**
     * 搜索内容
     * 1-食物
     * 2-游戏
     * 3-购物
     * 4-动物
     */
    public final static String[][] SEARCH_CONTENT = {
            {"苹果", "梨", "香蕉", "芒果", "西瓜", "桃子", "聚会", "冰淇淋", "餐厅", "火锅"},
            {"英雄联盟", "穿越火线", "和平精英", "王者荣耀", "弓箭手", "我的世界", "阴阳师", "迷你世界", "明日之后", "决战！平安京"},
            {"鞋子", "上衣", "T恤", "羽绒服", "短裤", "长裤", "手表", "帽子", "冰箱", "电视机"},
            {"猫", "狗", "狼", "鸡", "鸭", "鹅", "猪", "虎", "大象", "老鼠"}
    };


}
