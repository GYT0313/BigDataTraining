package com.cn.gp.db;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;


class DataBaseCommonTest {
    private Connection connection = null;
    private Statement statement = null;

    @BeforeEach
    void setUp() {
        connection = DataBaseCommon.getConnection("hive");
        statement = DataBaseCommon.getStatement("hive");
    }

    @AfterEach
    void tearDown() {
        DataBaseCommon.close(connection, statement);
    }

    @Test
    void getConnection() {
        Assert.assertNotNull(connection);
    }

    @Test
    void getStatement() {
        Assert.assertNotNull(statement);
    }

    @Test
    void close() {
        DataBaseCommon.close(connection);
    }

    @Test
    void testClose() {
    }

    @Test
    void testClose1() {
    }

    @Test
    void testClose2() {
    }
}