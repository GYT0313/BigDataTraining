package com.cn.gp.hbasequery.controller;

import com.cn.gp.hbasequery.service.HBaseQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author GuYongtao
 * @version 1.0.0
 * @className: HBaseQueryController
 * @description: controller类
 * @date 2019/10/7
 */

@Controller
@RequestMapping(value = "/hbase")
public class HBaseQueryController {
    private static Logger LOG = LoggerFactory.getLogger(HBaseQueryController.class);

    /**
     * 注入，通过这个注解可以直接拿到类的实例
     */
    @Resource
    private HBaseQueryService hBaseQueryService;

    @ResponseBody
    @RequestMapping(value = "/getHbase", method = {RequestMethod.GET, RequestMethod.POST})
    public Set<String> getHbase(@RequestParam(name = "table") String table,
                                @RequestParam(name = "rowKey") String rowKey) {
        return hBaseQueryService.getSingleColumn(table, rowKey);
    }


    @ResponseBody
    @RequestMapping(value = "/search1", method = {RequestMethod.GET, RequestMethod.POST})
    public Set<String> search1(@RequestParam(name = "table") String table,
                               @RequestParam(name = "rowKey") String rowKey) {
        //通过二级索引去找主关联表的rowkey 这个rowkey就是MAC
        return hBaseQueryService.getSingleColumn(table, rowKey);
    }


    @ResponseBody
    @RequestMapping(value = "/getRelation", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, List<String>> getRelation(@RequestParam(name = "field") String field,
                                                 @RequestParam(name = "fieldValue") String fieldValue) {

        return hBaseQueryService.getRelation(field, fieldValue);
    }

    public static void main(String[] args) {
        HBaseQueryController hBaseQueryController = new HBaseQueryController();
        Set<String> hbase = hBaseQueryController.getHbase("send_mail", "222222222@qq.com");
    }
}
