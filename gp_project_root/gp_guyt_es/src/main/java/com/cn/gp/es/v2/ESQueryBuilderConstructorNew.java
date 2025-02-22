package com.cn.gp.es.v2;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: 
 * @description: 查询条件容器
 * @Date:Created in 2018-04-09 18:46
 */
public class ESQueryBuilderConstructorNew {

    private List<String> highLighterFields = new ArrayList<String>();

    private int size = Integer.MAX_VALUE;

    private int from = 0;

    private List<SortBuilder> sortBuilderList;

    public List<SortBuilder> getSortBuilderList() {
        return sortBuilderList;
    }

    public void setSortBuilderList(List<SortBuilder> sortBuilderList) {
        this.sortBuilderList = sortBuilderList;
    }

    private Map<String,List<String>> sortMap;

    //查询条件容器
    private List<ESCriterion> mustCriterions = new ArrayList<ESCriterion>();
    private List<ESCriterion> shouldCriterions = new ArrayList<ESCriterion>();
    private List<ESCriterion> mustNotCriterions = new ArrayList<ESCriterion>();

    //构造builder
    public QueryBuilder listBuilders() {
        int count = mustCriterions.size() + shouldCriterions.size() + mustNotCriterions.size();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        QueryBuilder queryBuilder = null;

        if (count >= 1) {
            //must容器
            if (!CollectionUtils.isEmpty(mustCriterions)) {
                for (ESCriterion criterion : mustCriterions) {
                    for (QueryBuilder builder : criterion.listBuilders()) {
                        queryBuilder = boolQueryBuilder.must(builder);
                    }
                }
            }
            //should容器
            if (!CollectionUtils.isEmpty(shouldCriterions)) {
                for (ESCriterion criterion : shouldCriterions) {
                    for (QueryBuilder builder : criterion.listBuilders()) {
                        queryBuilder = boolQueryBuilder.should(builder);
                    }

                }
            }
            //must not 容器
            if (!CollectionUtils.isEmpty(mustNotCriterions)) {
                for (ESCriterion criterion : mustNotCriterions) {
                    for (QueryBuilder builder : criterion.listBuilders()) {
                        queryBuilder = boolQueryBuilder.mustNot(builder);
                    }
                }
            }
            return queryBuilder;
        } else {
            return null;
        }
    }

    /**
     * 增加简单条件表达式
     */
    public ESQueryBuilderConstructorNew must(ESCriterion criterion){
        if(criterion!=null){
            mustCriterions.add(criterion);
        }
        return this;
    }
    /**
     * 增加简单条件表达式
     */
    public ESQueryBuilderConstructorNew should(ESCriterion criterion){
        if(criterion!=null){
            shouldCriterions.add(criterion);
        }
        return this;
    }
    /**
     * 增加简单条件表达式
     */
    public ESQueryBuilderConstructorNew mustNot(ESCriterion criterion){
        if(criterion!=null){
            mustNotCriterions.add(criterion);
        }
        return this;
    }




    public List<String> getHighLighterFields() {
        return highLighterFields;
    }

    public void setHighLighterFields(List<String> highLighterFields) {
        this.highLighterFields = highLighterFields;
    }
    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Map<String, List<String>> getSortMap() {
        return sortMap;
    }

    public void setSortMap(Map<String, List<String>> sortMap) {
        this.sortMap = sortMap;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }




}
