package com.cn.gp.flume.source;

import com.cn.gp.flume.constant.FlumeConfConstant;
import com.cn.gp.flume.fields.MapFields;
import com.cn.gp.flume.utils.FileUtilsStronger;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 自定义source </p>
 * @date 2020/2/21
 */
public class FolderSource extends AbstractSource implements Configurable, PollableSource {

    private final Logger LOG = LoggerFactory.getLogger(FolderSource.class);

    // 读取的文件目录
    private String dirStr;
    // 读取的文件目录，如果多个，以,分割
    private String[] dirs;
    // 处理成功的文件写入的目录
    private String successFile;
    // 睡眠时间
    private long sleepTime = 5;
    // 每批文件数量
    private int fileNum = 500;

    // 以下为配置在txtparse.properties文件中
    // 读取的所有文件集合
    private Collection<File> allFiles;
    // 一批处理的文件大小
    private List<File> listFiles;
    private ArrayList<Event> eventList = new ArrayList<>();


    @Override
    public Status process() {
        LOG.info("Flume - Source开始执行...");
        try {
            Thread.sleep(sleepTime * 1000);
        } catch (InterruptedException e) {
            LOG.error(null, e);
        }

        Status status = null;
        try {
            // 获取目录下以"txt"结尾的文件
            allFiles = FileUtils.listFiles(new File(dirStr), new String[]{"txt"}, true);
            // 如果目录下文件数大于阈值, 则取fileNum 个文件处理
            if (allFiles.size() >= fileNum) {
                listFiles = ((List<File>) allFiles).subList(0, fileNum);
            } else {
                listFiles = ((List<File>) allFiles);
            }
            if (listFiles.size() > 0) {
                for (File file : listFiles) {
                    String fileName = file.getName();
                    // 解析文件: 文件名，文件内容，文件绝对路径
                    Map<String, Object> stringObjectMap = FileUtilsStronger.parseFile(file, successFile);
                    String absoluteFileName = (String) stringObjectMap.get(MapFields.ABSOLUTE_FILENAME);
                    List<String> lines = (List<String>) stringObjectMap.get(MapFields.VALUE);
                    if (lines != null && lines.size() > 0) {
                        for (String line : lines) {
                            // 封装event Header，将文件名及绝对路径通过header传送到channel
                            Map<String, String> map = new HashMap<>();
                            map.put(MapFields.FILENAME, fileName);
                            map.put(MapFields.ABSOLUTE_FILENAME, absoluteFileName);
                            // 构建event
                            SimpleEvent event = new SimpleEvent();
                            byte[] bytes = line.getBytes();
                            event.setBody(bytes);
                            event.setHeaders(map);
                            eventList.add(event);
                        }
                    }

                    try {
                        if (eventList.size() > 0) {
                            // 获取channelProcessor
                            ChannelProcessor channelProcessor = getChannelProcessor();
                            // 通过channelProcessor把eventLists发送
                            channelProcessor.processEventBatch(eventList);
                            LOG.info("从Source推送数据条数为: " + eventList.size());
                        }
                        eventList.clear();
                    } catch (Exception e) {
                        eventList.clear();
                        LOG.error("从Source推送数据失败", e);
                    } finally {
                        eventList.clear();
                    }
                }
            }
            // 处理成功
            status = Status.READY;
            return status;
        } catch (Exception e) {
            status = Status.BACKOFF;
            LOG.error("异常", e);
            return status;
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        // context能拿到flume配置文件的所有参数
        LOG.info("初始化Flume参数...");
        initFlumeParams(context);
        LOG.info("初始化Flume参数成功...");
    }

    /**
     * @return void
     * @author GuYongtao
     * <p>初始化Flume参数</p>
     * @date 2020/2/21
     */
    public void initFlumeParams(Context context) {
        try {
            //文件处理目录
            dirStr = context.getString(FlumeConfConstant.DIRS);
            dirs = dirStr.split(",");
            //成功处理文件存放目录
            successFile = context.getString(FlumeConfConstant.SUCCESSFILE);
            //每批处理文件个数
            fileNum = context.getInteger(FlumeConfConstant.FILENUM);
            //睡眠时间
            sleepTime = context.getLong(FlumeConfConstant.SLEEPTIME);
            LOG.info("dirStr============" + dirStr);
            LOG.info("dirs==============" + dirs);
            LOG.info("successfile=======" + successFile);
            LOG.info("filenum===========" + fileNum);
            LOG.info("sleeptime=========" + sleepTime);
        } catch (Exception e) {
            LOG.error("初始化Flume参数失败", e);
        }
    }
}
