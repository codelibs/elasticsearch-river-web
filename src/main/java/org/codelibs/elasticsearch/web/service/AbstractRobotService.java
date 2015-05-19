package org.codelibs.elasticsearch.web.service;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.codec.binary.Base64;
import org.codelibs.core.beans.BeanDesc;
import org.codelibs.core.beans.Converter;
import org.codelibs.core.beans.PropertyDesc;
import org.codelibs.core.beans.factory.BeanDescFactory;
import org.codelibs.core.beans.util.BeanUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.codelibs.robot.entity.AccessResult;
import org.codelibs.robot.entity.AccessResultDataImpl;
import org.codelibs.robot.exception.RobotSystemException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;

public abstract class AbstractRobotService {

    private static final String ID_SEPARATOR = ".";

    private static final Base64 base64 = new Base64(Integer.MAX_VALUE, new byte[0], true);

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    protected static final String SESSION_ID = "sessionId";

    protected static final String URL = "url";

    protected static final String LAST_MODIFIED = "lastModified";

    protected static final String CREATE_TIME = "createTime";

    protected static final String[] timestampFields = new String[] { LAST_MODIFIED, CREATE_TIME };

    protected String index;

    protected String type;

    @Resource
    protected RiverConfig riverConfig;

    @Resource
    protected Client esClient;

    protected XContentBuilder getXContentBuilder(final Object target) {
        try {
            return jsonBuilder().value(target);
        } catch (final IOException e) {
            throw new RobotSystemException("Failed to convert " + target + " to JSON.", e);
        }
    }

    protected void refresh() {
        esClient.admin().indices().prepareRefresh(index).execute().actionGet();
    }

    protected void insert(final Object target, final OpType opType) {
        final String id = getId(getSessionId(target), getUrl(target));
        final XContentBuilder source = getXContentBuilder(target);
        esClient.prepareIndex(index, type, id).setSource(source).setOpType(opType).setRefresh(true).execute().actionGet();
    }

    protected <T> void insertAll(final List<T> list, final OpType opType) {
        final BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        for (final T target : list) {
            final String id = getId(getSessionId(target), getUrl(target));
            final XContentBuilder source = getXContentBuilder(target);
            bulkRequest.add(esClient.prepareIndex(index, type, id).setSource(source).setOpType(opType));
        }
        final BulkResponse bulkResponse = bulkRequest.setRefresh(true).execute().actionGet();
        if (bulkResponse.hasFailures()) {
            throw new RobotSystemException(bulkResponse.buildFailureMessage());
        }
    }

    protected boolean exists(final String sessionId, final String url) {
        final String id = getId(sessionId, url);
        final GetResponse response = esClient.prepareGet(index, type, id).setRefresh(true).execute().actionGet();
        return response.isExists();
    }

    protected <T> T get(final Class<T> clazz, final String sessionId, final String url) {
        final String id = getId(sessionId, url);
        final GetResponse response = esClient.prepareGet(index, type, id).execute().actionGet();
        if (response.isExists()) {
            final Map<String, Object> source = response.getSource();
            final T bean = BeanUtil.copyMapToNewBean(source, clazz, option -> {
                option.exclude("accessResultData").converter(new EsTimestampConverter(), timestampFields).excludeWhitespace();
            });
            if (source.containsKey("accessResultData")) {
                ((AccessResult) bean).setAccessResultData(BeanUtil.copyMapToNewBean((Map<String, Object>) source.get("accessResultData"),
                        AccessResultDataImpl.class));
            }
            return bean;
        }
        return null;
    }

    protected <T> List<T> getList(final Class<T> clazz, final String sessionId, final QueryBuilder queryBuilder, final Integer from,
            final Integer size, final SortBuilder sortBuilder) {
        final List<T> targetList = new ArrayList<T>();
        final SearchResponse response = getSearchResponse(sessionId, queryBuilder, from, size, sortBuilder);
        final SearchHits hits = response.getHits();
        if (hits.getTotalHits() != 0) {
            try {
                for (final SearchHit searchHit : hits.getHits()) {
                    targetList.add(BeanUtil.copyMapToNewBean(searchHit.getSource(), clazz, option -> {
                        option.converter(new EsTimestampConverter(), timestampFields).excludeWhitespace();
                    }));
                }
            } catch (final Exception e) {
                throw new RobotSystemException("response: " + response, e);
            }
        }
        return targetList;
    }

    protected boolean delete(final String sessionId, final String url) {
        final String id = getId(sessionId, url);
        final DeleteResponse response = esClient.prepareDelete(index, type, id).setRefresh(true).execute().actionGet();
        return response.isFound();
    }

    protected void deleteBySessionId(final String sessionId) {
        esClient.prepareDeleteByQuery(index).setTypes(type).setQuery(QueryBuilders.queryStringQuery(SESSION_ID + ":" + sessionId))
                .execute().actionGet();
        refresh();
    }

    public void deleteAll() {
        esClient.prepareDeleteByQuery(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        refresh();
    }

    private SearchResponse getSearchResponse(final String sessionId, final QueryBuilder queryBuilder, final Integer from,
            final Integer size, final SortBuilder sortBuilder) {
        final SearchRequestBuilder builder = esClient.prepareSearch(index).setTypes(type);
        if (StringUtil.isNotBlank(sessionId)) {
            builder.setPostFilter(FilterBuilders.queryFilter(QueryBuilders.queryStringQuery(SESSION_ID + ":" + sessionId)));
        }
        if (queryBuilder != null) {
            builder.setQuery(queryBuilder);
        } else {
            builder.setQuery(QueryBuilders.matchAllQuery());
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
        return response;
    }

    private String getId(final String sessionId, final String url) {
        return sessionId + ID_SEPARATOR + new String(base64.encode(url.getBytes(UTF_8)), UTF_8);
    }

    private String getUrl(final Object target) {
        final BeanDesc beanDesc = BeanDescFactory.getBeanDesc(target.getClass());
        final PropertyDesc sessionIdProp = beanDesc.getPropertyDesc(URL);
        final Object sessionId = sessionIdProp.getValue(target);
        return sessionId == null ? null : sessionId.toString();
    }

    private String getSessionId(final Object target) {
        final BeanDesc beanDesc = BeanDescFactory.getBeanDesc(target.getClass());
        final PropertyDesc sessionIdProp = beanDesc.getPropertyDesc(SESSION_ID);
        final Object sessionId = sessionIdProp.getValue(target);
        return sessionId == null ? null : sessionId.toString();
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(final String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    protected static class EsTimestampConverter implements Converter {

        @Override
        public String getAsString(final Object value) {
            if (value instanceof Date) {
                return XContentBuilder.defaultDatePrinter.print(((Date) value).getTime());
            }
            return null;
        }

        @Override
        public Object getAsObject(final String value) {
            if (StringUtil.isEmpty(value)) {
                return null;
            }
            return new Timestamp(XContentBuilder.defaultDatePrinter.parseMillis(value));
        }

        @Override
        public boolean isTarget(@SuppressWarnings("rawtypes") final Class clazz) {
            return clazz == Date.class;
        }

    }

}
