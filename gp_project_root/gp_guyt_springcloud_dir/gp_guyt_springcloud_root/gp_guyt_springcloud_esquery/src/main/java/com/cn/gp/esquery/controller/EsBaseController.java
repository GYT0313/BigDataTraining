package com.cn.gp.esquery.controller;


import com.cn.gp.esquery.service.EsBaseService;
import com.cn.gp.esquery.feign.HBaseFeign;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping("/es")
public class EsBaseController {

    private static Logger LOG = LoggerFactory.getLogger(EsBaseController.class);

    @Resource
    private EsBaseService esBaseService;

    @Autowired
    private HBaseFeign hBaseFeign;

    /**
     * @param [indexName, typeName, sortField, sortValue, pageNumber, pageSize]
     * @return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
     * @method: getBaseInfo
     * @author GuYongtao
     * @description: es 基础查询
     * @date 2019/10/8
     */
    @ResponseBody
    @RequestMapping(value = "/getBaseInfo", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getBaseInfo(@RequestParam(name = "indexName") String indexName,
                                                 @RequestParam(name = "typeName") String typeName,
                                                 @RequestParam(name = "sortField") String sortField,
                                                 @RequestParam(name = "sortValue") String sortValue,
                                                 @RequestParam(name = "pageNumber") int pageNumber,
                                                 @RequestParam(name = "pageSize") int pageSize) {
        // 根据数据类型，排序，分页
        // 参数: indexName, typeName
        // 排序字段, sortField, sortValue
        // 分页数, 每页多大量, pageNumber, pageSize
        return esBaseService.getBaseInfo(indexName, typeName, sortField, sortValue, pageNumber, pageSize);
    }

    @ResponseBody
    @RequestMapping(value = "/getLocus", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getLocus(@RequestParam(name = "field") String field,
                                              @RequestParam(name = "fieldValue") String fieldValue) {
        Set<String> macSet = hBaseFeign.search1(field, fieldValue);
        // 根据数据类型，排序，分页
        // 参数: indexName, typeName
        // 排序字段, sortField, sortValue
        // 分页数, 每页多大量, pageNumber, pageSize
        // 先取第一条测试一下
        String mac = macSet.iterator().next();
        return esBaseService.getLocus(mac);
    }


}
