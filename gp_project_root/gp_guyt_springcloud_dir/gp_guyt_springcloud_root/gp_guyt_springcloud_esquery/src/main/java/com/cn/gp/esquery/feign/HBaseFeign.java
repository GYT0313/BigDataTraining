package com.cn.gp.esquery.feign;

import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;

@FeignClient("gp-guyt-hbasequery")
public interface HBaseFeign {

    @ResponseBody
    @RequestMapping(value = "/hbase/search1", method = {RequestMethod.GET})
    Set<String> search1(@RequestParam(name = "table") String table,
                        @RequestParam(name = "rowKey") String rowKey);

}
