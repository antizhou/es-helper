package com.iforfee.es.helper.service;

import com.iforfee.es.helper.Query;
import com.iforfee.es.helper.config.EsConfig;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @author joyo
 * @date 2017/11/2
 */
public class Helper {
    private static final Logger logger = LoggerFactory.getLogger(Helper.class);
    private final TransportClient client;

    public Helper(String servers, String clusterName) {
        client = EsConfig.getClint(servers, clusterName);
    }

    public SearchResponse search(Query query) {
        SearchRequestBuilder builder = client
                .prepareSearch(query.getIndex())
                .setTypes(query.getType());
        if (query.getQueryBuilder() != null) {
            builder.setQuery(query.getQueryBuilder());
        }
        if (query.getSize() != null) {
            builder.setSize(query.getSize());
        }
        for (AggregationBuilder aggregation : query.getAggregations()) {
            builder.addAggregation(aggregation);
        }
        Map<String, SortOrder> sorterMap = query.getSorter();
        if (sorterMap.size() > 0) {
            for (String field : sorterMap.keySet()) {
                builder.addSort(field, sorterMap.get(field));
            }
        }
        SearchResponse sr = builder.execute().actionGet();
        return sr;
    }

    public GetResponse search(String index, String type, String id) {
        return client.prepareGet(index, type, id).get();
    }


    public Map<String, Map<String, Long>> getFilterRules(String index, String type) {
        Map<String, Map<String, Long>> map = new HashMap<>();
        try {
            Query query = new Query();
            query.setIndex(index);
            query.setType(type);
            SearchResponse sr = this.search(query);

            SearchHits searchHits = sr.getHits();
            SearchHit[] hits = searchHits.getHits();

            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                Map<String, Object> source = hit.getSource();
                Object service = source.get("service");
                Object url = source.get("url");
                Object times = source.get("limit");

                Map<String, Long> rule = map.get(service == null ? "" : String.valueOf(service));
                if (rule == null) {
                    rule = new HashMap<>();
                }
                rule.put(url == null ? "" : (String) url, times == null ? 0 : ((Double) times).longValue());
            }
        } catch (Exception e) {
            logger.error("get api exception percent filters failed: {}", e.toString());
            e.printStackTrace();
        }
        return map;
    }

    public boolean insert(Query query) {
        IndexResponse response = client.prepareIndex(query.getIndex(), query.getType())
                .setSource(query.getSource(), XContentType.JSON)
                .get();
        String id = response.getId();
        if (id != null) {
            return true;
        }
        return false;
    }

    public boolean insert(String index, String type, String id, XContentBuilder source) {
        IndexRequestBuilder builder = client.prepareIndex(index, type, id);
        IndexResponse response = builder.setSource(source)
                .get();
        if (response.status().getStatus() == 201) {
            return true;
        }
        return false;
    }

    public List<SearchHits> scrollSearch(Query query) {
        long timeout = 120000;
        String scrollId = "";
        List<String> cursorIds = new LinkedList<>();

        SearchResponse scrollResp = client.prepareSearch(query.getIndex())
                .setTypes(query.getType())
                .setScroll(new TimeValue(timeout))
                .setQuery(query.getQueryBuilder())
                .setSize(10000).get();

        List<SearchHits> list = new LinkedList<>();
        do {
            list.add(scrollResp.getHits());
            scrollId = scrollResp.getScrollId();

            cursorIds.add(scrollId);

            scrollResp = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(timeout)).execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);

        clearScrollId(cursorIds);

        return list;
    }

    private boolean clearScrollId(List<String> scrollIds) {
        ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
        clearScrollRequestBuilder.setScrollIds(scrollIds);
        ClearScrollResponse response = clearScrollRequestBuilder.get();
        return response.isSucceeded();
    }

    public boolean bulkSave(Query query) {

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        List<Map<String, Object>> data = query.getBulk();
        data.stream().forEach(item -> {
            bulkRequest.add(client.prepareIndex(query.getIndex(), query.getType()).setSource(item));
        });
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            logger.error("buld save failed");
            return false;
        }
        return true;
    }

//    public boolean bulkObjectSave(EsQuery condition) {
//
//        BulkRequestBuilder bulkRequest = client.prepareBulk();
//
//        for (Object o : condition.getBulkObjects()) {
//            try {
//                String json = mapper.writeValueAsString(o);
//                bulkRequest.add(client.prepareIndex(condition.getIndex(), condition.getType()).setSource(json, XContentType.JSON));
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        }
//
//        BulkResponse bulkResponse = bulkRequest.get();
//        if (bulkResponse.hasFailures()) {
//            // process failures by iterating through each bulk response item
//            logger.error("buld save failed");
//            return false;
//        }
//        return true;
//    }

    public void deleteByQueryAsync(Query query) {

        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(query.getQueryBuilder())
                .source(query.getIndex())
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Handle the exception
                        logger.error("delete by query failed: {}", e.toString());
                    }
                });
    }


    public long deleteByQuery(Query query) {
        DeleteByQueryRequestBuilder builder = DeleteByQueryAction.INSTANCE.newRequestBuilder(client);

        SearchRequestBuilder source = builder.source();
        source.setQuery(query.getQueryBuilder());
        source.setTypes(query.getType());
        source.setIndices(query.getIndex());

        BulkByScrollResponse response = builder.get();

        return response.getDeleted();
    }

    public MultiSearchResponse multiSearch(List<Query> queries) {
        List<SearchRequestBuilder> builders = new LinkedList<>();

        queries.forEach(query -> {
            QueryBuilder queryBuilder = query.getQueryBuilder();
            SearchRequestBuilder sr = client
                    .prepareSearch().setQuery(queryBuilder).setSize(1);

            for (AggregationBuilder aggregation : query.getAggregations()) {
                sr.addAggregation(aggregation);
            }
            builders.add(sr);
        });

        MultiSearchRequestBuilder sr = client.prepareMultiSearch();
        builders.forEach(builder -> {
            sr.add(builder);
        });
        return sr.get();
    }
}
