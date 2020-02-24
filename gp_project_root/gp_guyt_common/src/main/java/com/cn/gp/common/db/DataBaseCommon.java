package com.cn.gp.common.db;

import com.cn.gp.common.config.ConfigUtil;
import com.cn.gp.common.fields.DataBaseFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 数据库工具类 </p>
 * @date 2020/1/13
 */
public class DataBaseCommon {
    /**
     * 静态变量
     */
    private static Logger LOG = LoggerFactory.getLogger(DataBaseCommon.class);
    private static Properties properties = ConfigUtil.getInstance().getProperties(DataBaseFields.MYSQL_PATH);
    public static Connection connection;

    private DataBaseCommon() {
    }

    /**
     * 从配置文件获取变量
     */
    private static final String USER_NAME = properties.getProperty(DataBaseFields.USER_NAME);
    private static final String PASSWORD = properties.getProperty(DataBaseFields.PASSWORD);
    private static final String IP = properties.getProperty(DataBaseFields.IP);
    private static final String PORT = properties.getProperty(DataBaseFields.PORT);

    static {
        try {
            Class.forName(DataBaseFields.JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            LOG.error(null, e);
        }
    }

    /**
     * @return java.sql.Connection
     * @author GuYongtao
     * <p> 获取数据库连接 </p>
     * @date 2020/1/13
     */
    public static Connection getConnection(String dbName) {
        Connection connection = null;
        String connectionString = "jdbc:mysql://" + IP + ":" + PORT + "/" + dbName + DataBaseFields.DB_CONFIG;
        try {
            connection = DriverManager.getConnection(connectionString, USER_NAME, PASSWORD);
        } catch (SQLException e) {
            LOG.error(null, e);
        }
        return connection;
    }

    /**
     * @return java.sql.Statement
     * @author GuYongtao
     * <p>创建数据库声明</p>
     * @date 2020/1/13
     */
    public static Statement getStatement(String dbName) {
        Statement statement = null;
        try {
            Connection connection = getConnection(dbName);
            if (connection != null) {
                statement = connection.createStatement();
            }
        } catch (SQLException e) {
            LOG.error(null, e);
        }
        return statement;
    }


    /**
     * @author GuYongtao
     * <p> 关闭数据库连接</p>
     * @date 2020/1/13
     */
    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.error(null, e);
            }
        }
    }

    /**
     * @author GuYongtao
     * <p> 关闭数据库声明</p>
     * @date 2020/1/13
     */
    public static void close(Statement statement) {
        try {
            if (connection != null) {
                statement.close();
            }
        } catch (SQLException e) {
            LOG.error(null, e);
        }
    }


    public static void close(Connection connection, Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }

        } catch (SQLException e) {
            LOG.error(null, e);
        }
    }

    public static void close(Connection connection, PreparedStatement preparedStatement) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }

        } catch (SQLException e) {
            LOG.error(null, e);
        }
    }


    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {

                connection.close();
            }
        } catch (SQLException e) {
            LOG.error(null, e);
        }
    }
}
