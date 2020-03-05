package com.cn.gp.common.fields;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 数据库常量字段 </p>
 * @date 2020/1/13
 */
public class DataBaseFields {
    /**
     * 配置文件路径
     */
    public static final String MYSQL_PATH = "common/mysql.properties";
    /**
     * 配置文件key名称
     */
    public static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String USER_NAME = "user";
    public static final String PASSWORD = "password";
    public static final String IP = "db_ip";
    public static final String PORT = "db_port";
    public static final String DB_CONFIG = "?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&failOverReadOnly=false";


}
