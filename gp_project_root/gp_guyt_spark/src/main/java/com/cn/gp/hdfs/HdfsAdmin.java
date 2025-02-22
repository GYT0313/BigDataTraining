package com.cn.gp.hdfs;


import com.cn.gp.common.adjuster.StringAdjuster;
import com.cn.gp.common.file.FileCommon;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> HDFS文件操作 </p>
 * @date 2020/2/23
 */
public class HdfsAdmin {
    private static Logger LOG = null;
    private static final String HDFS_SITE = "/hadoop/hdfs-site.xml";
    private static final String CORE_SITE = "/hadoop/core-site.xml";
    private volatile static HdfsAdmin hdfsAdmin;
    private FileSystem fileSystem;

    private HdfsAdmin(Configuration configuration, Logger logger) {
        try {
            if (configuration == null) {
                configuration = initConfiguration();
                configuration.set("fs.defaultFS", "hdfs://gp-guyt-1:8020");
                fileSystem = FileSystem.get(configuration);
            }
        } catch (IOException e) {
            if (LOG == null) {
                logger.error("获取HDFS的FileSystem失败.");
            } else {
                LOG.error("获取HDFS的FileSystem失败.");
            }
        }
        Preconditions.checkNotNull(fileSystem, "没有获取到可用HDFS的FileSystem.");
        LOG = logger;
        if (LOG == null) {
            LOG = LoggerFactory.getLogger(HdfsAdmin.class);
        }
    }

    /**
     * @return org.apache.hadoop.conf.Configuration
     * @author GuYongtao
     * <p>初始化configuration</p>
     * @date 2020/2/23
     */
    private Configuration initConfiguration() {
        Configuration configuration = new Configuration();
        if (FileCommon.exist(HDFS_SITE)) {
            configuration.addResource(HDFS_SITE);
        }
        if (FileCommon.exist(CORE_SITE)) {
            configuration.addResource(CORE_SITE);
        }
        return configuration;
    }

    /**
     * @return com.cn.gp.hdfs.HdfsAdmin
     * @author GuYongtao
     * <p>获取HdfsAdmin</p>
     * @date 2020/2/23
     */
    public static HdfsAdmin getInstance() {
        return getInstance(null);
    }

    public static HdfsAdmin getInstance(Logger logger) {
        if (hdfsAdmin == null) {
            synchronized (HdfsAdmin.class) {
                if (hdfsAdmin == null) {
                    hdfsAdmin = new HdfsAdmin(null, logger);
                }
            }
        }
        return hdfsAdmin;
    }

    public static HdfsAdmin getInstance(Configuration configuration, Logger logger) {
        if (hdfsAdmin == null) {
            synchronized (HdfsAdmin.class) {
                if (hdfsAdmin == null) {
                    hdfsAdmin = new HdfsAdmin(configuration, logger);
                }
            }
        }
        return hdfsAdmin;
    }

    /**
     * @return org.apache.hadoop.fs.FileStatus
     * @author GuYongtao
     * <p>获取文件信息</p>
     * @date 2020/2/23
     */
    public FileStatus getFileStatus(String dir) {
        FileStatus fileStatus = null;
        try {
            fileStatus = fileSystem.getFileStatus(new Path(dir));
        } catch (IOException e) {
            LOG.error("获取文件 %s信息失败" + dir, e);
        }
        return fileStatus;
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>新建文件</p>
     * @date 2020/2/23
     */
    public void createFile(String dst, byte[] contents) {
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream;
        try {
            outputStream = fileSystem.create(dstPath);
            outputStream.write(contents);
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.error(String.format("创建文件 %s 失败。", dst), e);
        }
        LOG.info(String.format("文件: %s 创建成功！", dst));
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>上传本地文件</p>
     * @date 2020/2/23
     */
    public void uploadFile(String src, String dst) {
        //原路径, 目标路径
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        try {
            fileSystem.copyFromLocalFile(false, srcPath, dstPath);
        } catch (IOException e) {
            LOG.error(String.format("上传文件 %s 到 %s 失败。", src, dst), e);
        }
        //打印文件路径
        LOG.info(String.format("上传文件 %s 到 %s 完成。", src, dst));
    }

    public void downloadFile(String src, String dst) {
        Path dstPath = new Path(dst);
        try {
            fileSystem.copyToLocalFile(false, new Path(src), dstPath);
        } catch (IOException e) {
            LOG.error(String.format("下载文件 %s 到 %s 失败。", src, dst), e);
        }
        LOG.info(String.format("下载文件 %s 到 %s 完成", src, dst));
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>文件重命名</p>
     * @date 2020/2/23
     */
    public void rename(String oldName, String newName) {

        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = false;
        try {
            isok = fileSystem.rename(oldPath, newPath);
        } catch (IOException e) {
            LOG.error(String.format("重命名文件 %s 为 %s 失败。", oldName, newName), e);
        }
        if (isok) {
            LOG.info(String.format("重命名文件 %s 为 %s 完成。", oldName, newName));
        } else {
            LOG.error(String.format("重命名文件 %s 为 %s 失败。", oldName, newName));
        }
    }

    public void delete(String path) {
        delete(path, true);
    }


    /**
     * @return void
     * @author GuYongtao
     * <p>删除文件</p>
     * @date 2020/2/23
     */
    public void delete(String path, boolean recursive) {

        Path deletePath = new Path(path);
        boolean isok = false;
        try {
            isok = fileSystem.delete(deletePath, recursive);
        } catch (IOException e) {
            LOG.error(String.format("删除文件 %s 失败。", path), e);
        }
        if (isok) {
            LOG.info(String.format("删除文件 %s 完成。", path));
        } else {
            LOG.error(String.format("删除文件 %s 失败。", path));
        }

    }

    /**
     * @return void
     * @author GuYongtao
     * <p>创建目录</p>
     * @date 2020/2/23
     */
    public void mkdir(String path) {

        Path srcPath = new Path(path);
        boolean isok = false;
        try {
            isok = fileSystem.mkdirs(srcPath);
        } catch (IOException e) {
            LOG.error(String.format("创建目录 %s 失败。", path), e);
        }
        if (isok) {
            LOG.info(String.format("创建目录 %s 完成。", path));
        } else {
            LOG.error(String.format("创建目录 %s 失败。", path));
        }
    }

    /**
     * @return java.io.InputStream
     * @author GuYongtao
     * <p>读取文件的内容</p>
     * @date 2020/2/23
     */
    public InputStream readFile(String filePath) {

        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fileSystem.open(srcPath);
        } catch (IOException e) {
            LOG.error(String.format("读取文件  %s 失败。", filePath), e);
        }
        return in;
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>读取文件内容</p>
     * @date 2020/2/23
     */
    public <T> void readFile(String filePath, StringAdjuster<T> adjuster, Collection<T> result) {

        InputStream inputStream = readFile(filePath);
        if (inputStream != null) {
            InputStreamReader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            try {
                T t;
                while ((line = bufferedReader.readLine()) != null) {
                    t = adjuster.doAdjust(line);
                    if (t != null) {
                        result.add(t);
                    }
                }
            } catch (IOException e) {
                LOG.error(String.format("利用缓冲流读取文件  %s 失败。", filePath), e);
            } finally {
                IOUtils.closeQuietly(bufferedReader);
                IOUtils.closeQuietly(reader);
                IOUtils.closeQuietly(inputStream);
            }
        }
    }

    public List<String> readLines(String filePath) {
        return readLines(filePath, "UTF-8");
    }

    public List<String> readLines(String filePath, String encoding) {
        InputStream inputStream = readFile(filePath);
        List<String> lines = null;
        if (inputStream != null) {
            try {
                lines = IOUtils.readLines(inputStream, encoding);
            } catch (IOException e) {
                LOG.error(String.format("按行读取文件 %s 失败。", filePath), e);
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
        }
        return lines;
    }

    public List<FileStatus> findNewFileOrDirInDir(String dir, HdfsFileFilter filter,
                                                  final boolean onlyFile, final boolean onlyDir) {
        return findNewFileOrDirInDir(dir, filter, onlyFile, onlyDir, false);
    }

    public List<FileStatus> findNewFileOrDirInDir(String dir, HdfsFileFilter filter,
                                                  final boolean onlyFile, final boolean onlyDir, boolean recursive) {

        if (onlyFile && onlyDir) {
            FileStatus fileStatus = getFileStatus(dir);
            if (fileStatus == null) {
                return Lists.newArrayList();
            }
            if (isAccepted(fileStatus, filter)) {
                return Lists.newArrayList(fileStatus);
            }
            return Lists.newArrayList();
        }

        if (onlyFile) {
            return findNewFileInDir(dir, filter, recursive);
        }
        if (onlyDir) {
            return findNewDirInDir(dir, filter, recursive);
        }

        return Lists.newArrayList();
    }

    /**
     * @return java.util.List<org.apache.hadoop.fs.FileStatus>
     * @author GuYongtao
     * <p>查找一个文件夹中 新建的目录</p>
     * @date 2020/2/23
     */
    public List<FileStatus> findNewDirInDir(String dir, HdfsFileFilter filter) {
        return findNewDirInDir(new Path(dir), filter, false);
    }

    public List<FileStatus> findNewDirInDir(Path path, HdfsFileFilter filter) {
        return findNewDirInDir(path, filter, false);
    }

    public List<FileStatus> findNewDirInDir(String dir, HdfsFileFilter filter, boolean recursive) {
        return findNewDirInDir(new Path(dir), filter, recursive);
    }

    public List<FileStatus> findNewDirInDir(Path path, HdfsFileFilter filter, boolean recursive) {

        FileStatus[] files = null;
        try {
            files = fileSystem.listStatus(path);
        } catch (IOException e) {
            LOG.error(String.format("获取目录 %s下的文件列表失败。", path), e);
        }
        if (files == null) return Lists.newArrayList();

        List<FileStatus> paths = Lists.newArrayList();
        List<String> res = Lists.newArrayList();
        for (FileStatus fileStatus : files) {
            if (fileStatus.isDirectory()) {
                if (isAccepted(fileStatus, filter)) {
                    paths.add(fileStatus);
                    res.add(fileStatus.getPath().toString());
                } else if (recursive) {
                    paths.addAll(findNewDirInDir(fileStatus.getPath(), filter, recursive));
                }
            }
        }
        LOG.info(String.format("从目录%s 找到满足条件%s 有如下 %s 个文件： %s",
                path.toString(), ((Object) filter).toString(), res.size(), res));
        return paths;
    }

    /**
     * @return java.util.List<org.apache.hadoop.fs.FileStatus>
     * @author GuYongtao
     * <p>查找一个文件夹中 新建的文件</p>
     * @date 2020/2/23
     */
    public List<FileStatus> findNewFileInDir(String dir, HdfsFileFilter filter) {
        return findNewFileInDir(new Path(dir), filter, false);
    }

    public List<FileStatus> findNewFileInDir(String dir, HdfsFileFilter filter, boolean recursive) {
        return findNewFileInDir(new Path(dir), filter, recursive);
    }

    public List<FileStatus> findNewFileInDir(Path path, HdfsFileFilter filter) {
        return findNewFileInDir(path, filter, false);
    }

    public List<FileStatus> findNewFileInDir(Path path, HdfsFileFilter filter, boolean recursive) {

        FileStatus[] files = null;
        try {
            files = fileSystem.listStatus(path);
        } catch (IOException e) {
            LOG.error(String.format("获取目录 %s下的文件列表失败。", path), e);
        }
        if (files == null) return Lists.newArrayList();

        List<FileStatus> paths = Lists.newArrayList();
        List<String> res = Lists.newArrayList();
        for (FileStatus fileStatus : files) {
            if (fileStatus.isFile()) {
                if (isAccepted(fileStatus, filter)) {
                    paths.add(fileStatus);
                    res.add(fileStatus.getPath().toString());
                }
            } else if (recursive) {
                paths.addAll(findNewFileInDir(fileStatus.getPath(), filter, recursive));
            }
        }
        LOG.info(String.format("从目录%s 找到满足条件%s 有如下 %s 个文件： %s", path, filter, res.size(), res));

        return paths;
    }

    private boolean isAccepted(String file, HdfsFileFilter filter) {
        if (filter == null) {
            return true;
        }
        FileStatus fileStatus = getFileStatus(file);
        if (fileStatus == null) {
            return false;
        }
        return isAccepted(fileStatus, filter);
    }

    private boolean isAccepted(FileStatus fileStatus, HdfsFileFilter filter) {
        return filter == null ? true : filter.filter(fileStatus);
    }

    public long getModificationTime(Path path) {
        try {
            FileStatus status = fileSystem.getFileStatus(path);
            return status.getModificationTime();
        } catch (IOException e) {
            LOG.error(String.format("获取路径 %s信息失败。", path), e);
        }
        return -1L;
    }

    public FileSystem getFs() {
        return fileSystem;
    }


}
