package com.cn.gp.esquery.service;


import com.cn.guyt.es.jest.service.JestService;
import com.cn.guyt.es.jest.service.ResultParse;
import io.searchbox.client.JestClient;
import io.searchbox.core.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class EsBaseService {

    private static Logger LOG = LoggerFactory.getLogger(EsBaseService.class);


    // 参数: indexName, typeName
    // 排序字段, sortField, sortValue
    // 分页数, 每页多大量, pageNumber, pageSize
    public List<Map<String, Object>> getBaseInfo(String indexName, String typeName,
                                                 String sortField, String sortValue,
                                                 int pageNumber, int pageSize) {
        // 实现查询
        JestClient jestClient = null;
        List<Map<String, Object>> maps = null;
        try {
            jestClient = JestService.getJestClient();
            SearchResult search = JestService.search(jestClient,
                    indexName,
                    typeName,
                    "",
                    "",
                    sortField,
                    sortValue,
                    pageNumber,
                    pageSize);
            maps = ResultParse.parseSearchResultOnly(search);
        } catch (Exception e) {
            LOG.error(null, e);
        } finally {
            JestService.closeJestClient(jestClient);
        }
        return maps;
    }


    /**
     * @param [phoneMac]
     * @return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
     * @method: getLocus
     * @author GuYongtao
     * @description: 时间轨迹数据
     * @date 2019/10/9
     */
    // 可以传一个时间范围
    public List<Map<String, Object>> getLocus(String phoneMac) {

        // 实现查询
        JestClient jestClient = null;
        List<Map<String, Object>> maps = null;
        String[] includes = new String[]{"latitude", "longitude", "collect_time"};
        try {
            jestClient = JestService.getJestClient();
            SearchResult search = JestService.search(jestClient,
                    "",
                    "",
                    "phone_mac.keyword",
                    phoneMac,
                    "collect_time",
                    "asc",
                    1,
                    2000,
                    includes);
            maps = ResultParse.parseSearchResultOnly(search);
        } catch (Exception e) {
            LOG.error(null, e);
        } finally {
            JestService.closeJestClient(jestClient);
        }
        return maps;
    }


}
