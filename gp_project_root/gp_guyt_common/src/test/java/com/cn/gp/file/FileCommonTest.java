package com.cn.gp.file;

import com.cn.gp.common.file.FileCommon;
import org.junit.Assert;
import org.junit.jupiter.api.Test;


class FileCommonTest {

    @Test
    void exist() {
        Assert.assertTrue(FileCommon.exist("G:\\IDEA_project\\scala-2.10.4.zip"));
    }

}