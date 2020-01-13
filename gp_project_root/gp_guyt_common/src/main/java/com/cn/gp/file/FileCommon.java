package com.cn.gp.file;

import com.cn.gp.fields.CommonFields;
import org.apache.commons.io.IOUtils;
import sun.plugin.cache.OldCacheEntry;

import java.io.*;
import java.util.List;
import java.util.Locale;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 文件工具类 </p>
 * @date 2020/1/13
 */
public class FileCommon {
    private FileCommon() {

    }

    /**
     * @return boolean
     * @author GuYongtao
     * <p>判断文件是否存在</p>
     * @date 2020/1/13
     */
    public static boolean exist(String fileName) {
        return exist(new File(fileName));
    }

    public static boolean exist(File file) {
        return file.exists();
    }

    /**
     * @return boolean
     * @author GuYongtao
     * <p>创建文件</p>
     * @date 2020/1/13
     */
    public static boolean createFile(String fileName) {
        return createFile(new File(fileName));
    }

    public static boolean createFile(File file) throws IOException {
        if (!file.exists()) {
            if (file.isDirectory()) {
                return file.mkdirs();
            } else {
                File parentDir = file.getParentFile();
                if (!parentDir.exists()) {
                    parentDir.mkdirs();
                }
                return file.createNewFile();
            }
            return false;
        }
    }


    /**
     * @return java.util.List<java.lang.String>
     * @author GuYongtao
     * <p>读取文件内容：按行读取</p>
     * @date 2020/1/13
     */
    public static List<String> readLines(String fileName) {
        return readLines(fileName, CommonFields.UTF8);
    }

    public static List<String> readLines(String fileName, String enCoding) {
        return readLines(new File(fileName), enCoding);
    }

    public static List<String> readLines(File file, String enCoding) throws IOException {
        List<String> lines = null;
        if (FileCommon.exist(file)) {
            FileInputStream fileInputStream = new FileInputStream(file);
            lines = IOUtils.readLines(fileInputStream, enCoding);
            fileInputStream.close();
        }
        return lines;
    }


    /**
     * @return java.lang.String
     * @author GuYongtao
     * <p>获取文件前缀</p>
     * @date 2020/1/13
     */
    public static String getPrefix(String fileName) {
        String prefix = fileName;
        int pos = fileName.lastIndexOf(".");
        if (pos != -1) {
            prefix = fileName.substring(0, pos);
        }
        return prefix;
    }

    /**
     * @return java.lang.String
     * @author GuYongtao
     * <p>获取的文件后缀</p>
     * @date 2020/1/13
     */
    public static String getFilePostfix(String fileName) {
        String filePostfix = fileName.substring(fileName.lastIndexOf(".") + 1);
        return filePostfix.toLowerCase();
    }

    /**
     * @return boolean
     * @author GuYongtao
     * <p>删除文件</p>
     * @date 2020/1/13
     */
    public static boolean delFile(String filePath) {
        boolean flag = false;
        File file = new File(filePath);
        if (file.isFile() && file.exists()) {
            flag = file.delete();
        }
        return flag;
    }

    /**
     * @return boolean
     * @author GuYongtao
     * <p>移动文件</p>
     * @date 2020/1/13
     */
    public static boolean mvFile(String oldPath, String newPath) {
        boolean flag = false;
        File oldFile = new File(oldPath);
        File newFile = new File(newPath);
        if (oldFile.isFile() && oldFile.exists()) {
            if (newFile.exists()) {
                delFile(newFile.getAbsolutePath());
            }
            flag = oldFile.renameTo(newFile);
        }
        return flag;
    }

    /**
     * @return boolean
     * @author GuYongtao
     * <p>删除目录/p>
     * @date 2020/1/13
     */
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            // 递归删除子目录
            if (children != null) {
                for (String childrenPath : children) {
                    boolean success = deleteDir(new File(dir, childrenPath));
                    if (!success) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>创建目录</p>
     * @date 2020/1/13
     */
    public static void mkdirs(File file) {
        File parent = file.getParentFile();
        if (parent != null && (!parent.exists())) {
            parent.mkdirs();
        }
    }

















}
