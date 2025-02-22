package com.cn.gp.spark.warn.dao;

import com.cn.gp.common.db.DataBaseCommon;
import com.cn.gp.spark.warn.domain.RuleDomain;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 从mysql查询警告规则 </p>
 * @date 2020/2/23
 */
public class RuleDao {
    private static final Logger LOG = LoggerFactory.getLogger(RuleDao.class);

    /**
     * @return java.util.List<RuleDomain>
     * @author GuYongtao
     * <p>获取所有规则</p>
     * @date 2020/2/23
     */
    public static List<RuleDomain> getRuleList() {
        List<RuleDomain> listRules = null;
        // 获取MySQL连接
        Connection connection = DataBaseCommon.getConnection("gp_warn_rules");
        // 执行器
        QueryRunner queryRunner = new QueryRunner();
        String sql = "select * from rules";
        try {
            listRules = queryRunner.query(connection, sql, new BeanListHandler<>(RuleDomain.class));
        } catch (SQLException e) {
            LOG.error(null, e);
        } finally {
            DataBaseCommon.close(connection);
        }
        return listRules;
    }

    public static void main(String[] args) {
        List<RuleDomain> ruleList = getRuleList();
        RuleDomain ruleDomain = ruleList.get(0);
        System.out.println(ruleDomain);
    }

}
