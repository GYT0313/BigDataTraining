package com.cn.gp.flume.utils;

import com.cn.gp.flume.fields.MapFields;
import com.cn.gp.common.time.TimeTranstationUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.io.File.separator;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p>  </p>
 * @date 2020/2/21
 */
public class FileUtilsStronger {

    private static final Logger logger = LoggerFactory.getLogger(FileUtilsStronger.class);

    /**
     * @param file
     * @param path
     */
    public static Map<String, Object> parseFile(File file, String path) {

        Map<String, Object> map = new HashMap<>(16);
        List<String> lines;
        // 增加时间+父级目录作为上层目录
        String fileNew = path + TimeTranstationUtils.Date2yyyy_MM_dd() + getDir(file);

        try {
            if ((new File(fileNew + file.getName())).exists()) {
                try {
                    logger.info("文件名已经存在，开始删除同名已经存在文件" + file.getAbsolutePath());
                    file.delete();
                    logger.info("删除同名已经存在文件" + file.getAbsolutePath() + "成功");
                } catch (Exception e) {
                    logger.error("删除同名已经存在文件" + file.getAbsolutePath() + "失败", e);
                }
            } else {
                lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
                map.put(MapFields.ABSOLUTE_FILENAME, fileNew + file.getName());
                map.put(MapFields.VALUE, lines);
                FileUtils.moveToDirectory(file, new File(fileNew), true);
                logger.info("移动文件到" + file.getAbsolutePath() + "到" + fileNew + "成功");
            }
        } catch (Exception e) {

            logger.error("移动文件" + file.getAbsolutePath() + "到" + fileNew + "失败", e);
        }

        return map;

    }


    /**
     * @param file
     * @param path
     */
    public static List<String> chanmodName(File file, String path) {

        List<String> lines = null;

        try {
            if ((new File(path + TimeTranstationUtils.Date2yyyy_MM_dd() + "/" + file.getName())).exists()) {
                logger.warn("文件名已经存在，开始删除同名文件" + path + TimeTranstationUtils.Date2yyyy_MM_dd() + "/" + file.getName());
                try {
                    file.delete();
                    logger.warn("删除同名文件" + file.getAbsolutePath() + "成功");
                } catch (Exception e) {
                    logger.warn("删除同名文件" + file.getAbsolutePath() + "失败", e);
                }
            } else {
                lines = FileUtils.readLines(file);
                FileUtils.moveToDirectory(file, new File(path + TimeTranstationUtils.Date2yyyy_MM_dd()), true);
                logger.info("移动文件到" + file.getAbsolutePath() + "到" + path + TimeTranstationUtils.Date2yyyy_MM_dd() + "成功");

            }
        } catch (Exception e) {

            logger.error("移动文件" + file.getName() + "到" + path + TimeTranstationUtils.Date2yyyy_MM_dd() + "失败", e);
        }

        return lines;
    }


    /**
     * @param file
     * @param path
     */
    public static void moveFile2unmanage(File file, String path) {

        try {
            if ((new File(path + TimeTranstationUtils.Date2yyyy_MM_dd() + "/" + file.getName())).exists()) {
                logger.warn("文件名已经存在，开始删除同名文件" + file.getAbsolutePath());
                try {
                    file.delete();
                    logger.warn("删除同名文件" + file.getAbsolutePath() + "成功");
                } catch (Exception e) {
                    logger.warn("删除同名文件" + file.getAbsolutePath() + "失败", e);
                }
            } else {
                FileUtils.moveToDirectory(file, new File(path + TimeTranstationUtils.Date2yyyy_MM_dd()), true);
                //logger.info("移动文件到"+file.getAbsolutePath()+"到"+path+ TimeTranstationUtils.Date2yyyy_MM_dd()+"成功");
            }
        } catch (Exception e) {

            logger.error("移动错误文件" + file.getName() + "到" + path + TimeTranstationUtils.Date2yyyy_MM_dd() + "失败", e);
        }
    }


    /**
     * @param file
     * @param path
     */
    public static void shnegtingChanmodName(File file, String path) {
        try {
            if ((new File(path + TimeTranstationUtils.Date2yyyy_MM_dd() + "/" + file.getName())).exists()) {
                logger.warn("文件名已经存在，开始删除同名文件" + path + TimeTranstationUtils.Date2yyyy_MM_dd() + "/" + file.getName());
                try {
                    file.delete();
                    logger.warn("删除同名文件" + file.getAbsolutePath() + "成功");
                } catch (Exception e) {
                    logger.warn("删除同名文件" + file.getAbsolutePath() + "失败", e);
                }
            } else {
                FileUtils.moveToDirectory(file, new File(path + TimeTranstationUtils.Date2yyyy_MM_dd()), true);
                logger.info("移动文件到" + file.getAbsolutePath() + "到" + path + TimeTranstationUtils.Date2yyyy_MM_dd() + "成功");

            }
        } catch (Exception e) {

            logger.error("移动文件" + file.getName() + "到" + path + TimeTranstationUtils.Date2yyyy_MM_dd() + "失败", e);
        }
    }


    /**
     * 获取文件父目录
     *
     * @param file
     * @return
     */
    public static String getDir(File file) {
        String dir = file.getParent();
        StringTokenizer dirs = new StringTokenizer(dir, separator);
        List<String> list = new ArrayList<>();
        while (dirs.hasMoreTokens()) {
            list.add((String) dirs.nextElement());
        }
        String str = "";
        for (int i = 2; i < list.size(); i++) {
            str = str + separator + list.get(i);
        }
        return str + "/";
    }

}
