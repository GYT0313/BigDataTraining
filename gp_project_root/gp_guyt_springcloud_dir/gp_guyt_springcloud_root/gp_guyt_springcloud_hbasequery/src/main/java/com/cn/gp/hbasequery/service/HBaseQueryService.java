package com.cn.gp.hbasequery.service;


import com.cn.gp.hbase.entity.HBaseCell;
import com.cn.gp.hbase.entity.HBaseRow;
import com.cn.gp.hbase.extractor.MultiVersionRowExtrator;
import com.cn.gp.hbase.extractor.SingleColumnMultiVersionRowExtrator;
import com.cn.gp.hbase.search.HBaseSearchService;
import com.cn.gp.hbase.search.HBaseSearchServiceImpl;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author GuYongtao
 * @version 1.0.0
 * @className: HBaseQueryService
 * @description: 服务类
 * @date 2019/10/7
 */
@Service
public class HBaseQueryService {

    private static Logger LOG = LoggerFactory.getLogger(HBaseQueryService.class);


    @Resource
    private HBaseQueryService hBaseQueryService;

    /**
     * @return java.util.Set<java.lang.String>
     * @author GuYongtao
     * <p>获取单列数据的多版本信息</p>
     * @date 2020/3/7
     */
    public Set<String> getSingleColumn(String field, String rowKey) {
        //从索引表中获取总关联表的rowkey  获取phone对应的多版本 MAC
        HBaseSearchService baseSearchService = new HBaseSearchServiceImpl();
        // 命名空间+表名
        String table = "test:" + field;
        Get get = new Get(rowKey.getBytes());
        Set<String> search = null;
        try {
            // 设置版本
            get.setMaxVersions(100);
            Set set = new HashSet<String>();
            SingleColumnMultiVersionRowExtrator extractor = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), set);
            // 获取phone表下多版本的phone_mac
            search = baseSearchService.search(table, get, extractor);
        } catch (IOException e) {
            LOG.error(null, e);
        }
        return search;
    }

    /**
     * @return java.util.Map<java.lang.String,java.util.List<java.lang.String>>
     * @author GuYongtao
     * <p>通过字段获取整条记录</p>
     * @date 2020/3/7
     */
    public Map<String, List<String>> getRelation(String field, String fieldValue) {
        Map<String, List<String>> mapResult = new HashMap<>();
        // 1. 从二级索引表中找到多版本rowkey
        // 命名空间+表名
        String table = field;
        String rowKey = fieldValue;
//        HBaseQueryService hBaseQueryService = new HBaseQueryService();
        Set<String> relationRowKeys = hBaseQueryService.getSingleColumn(field, rowKey);

        // 2. 拿到二级索引表中得到的主关联表的rowkey，然后遍历，获得多版本数据
        // 将rowKeys封装成List<Get>
        List<Get> list = new ArrayList<>();
        relationRowKeys.forEach(relationRowKey -> {
            Get get = new Get(relationRowKey.getBytes());
            try {
                get.setMaxVersions(100);
            } catch (IOException e) {
                LOG.error(null, e);
            }
            list.add(get);
        });

        // 通过relationRowKey，找test:relationh中所有信息
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
        String relationTable = "test:relation";

        MultiVersionRowExtrator multiVersionRowExtrator = new MultiVersionRowExtrator();
        // String tableName, List<Get> gets, RowExtractor<T> extractor
        try {
            List<HBaseRow> search = hBaseSearchService.search(relationTable, list, multiVersionRowExtrator);
            // 进行解析
            search.forEach(hBaseRow -> {
                Map<String, Collection<HBaseCell>> cellMap = hBaseRow.getCell();
                cellMap.forEach((key, value) -> {
                    // 转为Map<String, List<String>>
                    List<String> listValue = new ArrayList<>();
                    value.forEach(hBaseCell -> {
                        listValue.add(hBaseCell.toString());
                    });
                    mapResult.put(key, listValue);
                });
            });
        } catch (IOException e) {
            LOG.error(null, e);
        }
        System.out.println(mapResult);
        return mapResult;
    }


    public static void main(String[] args) {
        HBaseQueryService hBaseQueryService = new HBaseQueryService();
        hBaseQueryService.getRelation("send_mail", "222222222@qq.com");
    }
}
