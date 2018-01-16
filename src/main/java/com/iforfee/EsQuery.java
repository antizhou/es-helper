package com.iforfee;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author zhoujun
 * @date 2017/11/2
 */
public class EsQuery {
    private String index;
    private String type;
    private Integer size;
    private QueryBuilder queryBuilder;
    private List<AggregationBuilder> aggregations;
    private Map<String, Object> source = new HashMap<>();
    private List<Map<String, Object>> bulk = new LinkedList<>();
    private Map<String, SortOrder> sorter = new HashMap<>();
    private Object[] bulkObjects;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public void setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public List<AggregationBuilder> getAggregations() {
        return aggregations;
    }

    public void setAggregations(List<AggregationBuilder> aggregations) {
        this.aggregations = aggregations;
    }

    public void appendAggregation(AggregationBuilder aggregation) {
        if (this.aggregations == null) {
            this.aggregations = new LinkedList<>();
        }
        this.aggregations.add(aggregation);
    }

    public void setSorter(Map<String, SortOrder> sorter) {
        this.sorter = sorter;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    public List<Map<String, Object>> getBulk() {
        return bulk;
    }

    public void setBulk(List<Map<String, Object>> bulk) {
        this.bulk = bulk;
    }

    public void addSorter(String field, SortOrder sorter) {
        this.sorter.put(field, sorter);
    }

    public Map<String, SortOrder> getSorter() {
        return sorter;
    }

    public Object[] getBulkObjects() {
        return this.bulkObjects;
    }

    public void setBulkObjects(Object... bulkObjects) {
        this.bulkObjects = bulkObjects;
    }
}


