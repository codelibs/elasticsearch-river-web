package org.codelibs.elasticsearch.web.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.codelibs.elasticsearch.web.util.IdUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.seasar.framework.beans.util.Beans;
import org.seasar.robot.RobotSystemException;

import com.fasterxml.jackson.core.JsonProcessingException;

public abstract class AbstractRobotService {

    protected static final QueryStringQueryBuilder allDataQuery = QueryBuilders
            .queryString("*:*");

    protected static final String BASIC_DATE_TIME = "yyyyMMdd'T'HHmmss.SSSZ";

    protected static final String SESSION_ID = "sessionId";

    protected static final String[] timestampFields = new String[] {
            "lastModified", "createTime" };

    protected String index;

    @Resource
    protected RiverConfig riverConfig;

    protected String getJsonString(final Object target) {
        try {
            return riverConfig.getObjectMapper().writeValueAsString(target);
        } catch (final JsonProcessingException e) {
            throw new RobotSystemException("Failed to convert " + target
                    + " to JSON.", e);
        }
    }

    protected void refresh() {
        riverConfig.getClient().admin().indices().prepareRefresh(index)
                .execute().actionGet();
    }

    protected void insert(final Object target) {
        final String id = IdUtil.getId(target);
        final String type = IdUtil.getType(target);
        final String source = getJsonString(target);
        riverConfig.getClient().prepareIndex(index, type, id).setSource(source)
                .setRefresh(true).execute().actionGet();
    }

    protected <T> void insertAll(final List<T> list) {
        final BulkRequestBuilder bulkRequest = riverConfig.getClient()
                .prepareBulk();
        for (final T target : list) {
            final String id = IdUtil.getId(target);
            final String type = IdUtil.getType(target);
            final String source = getJsonString(target);
            bulkRequest.add(riverConfig.getClient()
                    .prepareIndex(index, type, id).setSource(source));
        }
        final BulkResponse bulkResponse = bulkRequest.setRefresh(true)
                .execute().actionGet();
        if (bulkResponse.hasFailures()) {
            throw new RobotSystemException(bulkResponse.buildFailureMessage());
        }
    }

    protected boolean exists(final String sessionId, final String id) {
        final GetResponse response = riverConfig.getClient()
                .prepareGet(index, sessionId, id).execute().actionGet();
        return response.isExists();
    }

    protected <T> T get(final Class<T> clazz, final String sessionId,
            final String url) {
        final String id = IdUtil.getId(url);
        final GetResponse response = riverConfig.getClient()
                .prepareGet(index, sessionId, id).execute().actionGet();
        if (response.isExists()) {
            return Beans.createAndCopy(clazz, response.getSource())
                    .timestampConverter(BASIC_DATE_TIME, timestampFields)
                    .excludesWhitespace().execute();
        }
        return null;
    }

    protected <T> List<T> getList(final Class<T> clazz, final String sessionId,
            final QueryBuilder queryBuilder, final Integer from,
            final Integer size, final SortBuilder sortBuilder) {
        final List<T> targetList = new ArrayList<T>();
        final SearchRequestBuilder builder = riverConfig.getClient()
                .prepareSearch(index).setTypes(sessionId);
        if (queryBuilder != null) {
            builder.setQuery(queryBuilder);
        } else {
            builder.setQuery(allDataQuery);
        }
        if (sortBuilder != null) {
            builder.addSort(sortBuilder);
        }
        if (from != null) {
            builder.setFrom(from);
        }
        if (size != null) {
            builder.setSize(size);
        }
        final SearchResponse response = builder.execute().actionGet();
        final SearchHits hits = response.getHits();
        if (hits.getTotalHits() != 0) {
            for (final SearchHit searchHit : hits.getHits()) {
                targetList.add(Beans
                        .createAndCopy(clazz, searchHit.getSource())
                        .timestampConverter(BASIC_DATE_TIME, timestampFields)
                        .excludesWhitespace().execute());
            }
        }
        return targetList;
    }

    protected void delete(final String sessionId, final String id) {
        riverConfig.getClient().prepareDelete(index, sessionId, id)
                .setRefresh(true).execute().actionGet();
    }

    protected void deleteBySessionId(final String sessionId) {
        riverConfig.getClient().prepareDeleteByQuery(index).setTypes(sessionId)
                .setQuery(allDataQuery).execute().actionGet();
        refresh();
    }

    public void deleteAll() {
        riverConfig.getClient().prepareDeleteByQuery(index)
                .setQuery(allDataQuery).execute().actionGet();
        refresh();
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(final String index) {
        this.index = index;
    }

}