package com.cn.gp.spark.warn.dao;

import com.cn.gp.spark.warn.domain.WarningMessage;
import com.cn.gp.common.db.DataBaseCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/2/24
 */
public class WarningMessageDao {
    private static final Logger LOG = LoggerFactory.getLogger(WarningMessageDao.class);

    /**
     * @return java.lang.Integer
     * @author GuYongtao
     * <p>写消息到MySQL</p>
     * @date 2020/2/23
     */
    public static Integer insertWarningMessageReturnId(WarningMessage warningMessage) {
        Connection connection = DataBaseCommon.getConnection("gp_warn_rules");
        String sql = "insert into warn_message(alarmruleid,sendtype,senfinfo,hittime,sendmobile,alarmtype) " +
                "values(?,?,?,?,?,?)";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        int id = -1;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, warningMessage.getAlarmRuleid());
            preparedStatement.setInt(2, Integer.valueOf(warningMessage.getSendType()));
            preparedStatement.setString(3, warningMessage.getSenfInfo());
            preparedStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setString(5, warningMessage.getSendMobile());
            preparedStatement.setInt(6, Integer.valueOf(warningMessage.getAlarmType()));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOG.error(null, e);
        } finally {
            DataBaseCommon.close(connection, preparedStatement, resultSet);
        }
        return id;
    }

}
