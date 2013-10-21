package org.codelibs.elasticsearch.web.robot.service;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.seasar.framework.beans.util.Beans;
import org.seasar.robot.entity.AccessResult;
import org.seasar.robot.entity.AccessResultImpl;
import org.seasar.robot.service.DataService;
import org.seasar.robot.util.AccessResultCallback;

public class EsDataService extends AbstractRobotService implements DataService {

    public int scrollTimeout = 60000;

    public int scrollSize = 100;

    @Override
    public void store(final AccessResult accessResult) {
        super.insert(accessResult);
    }

    @Override
    public void update(final AccessResult accessResult) {
        store(accessResult);
    }

    @Override
    public void update(final List<AccessResult> accessResultList) {
        insertAll(accessResultList);
    }

    @Override
    public int getCount(final String sessionId) {
        return (int) riverConfig.getClient().prepareCount(index)
                .setTypes(sessionId).execute().actionGet().getCount();
    }

    @Override
    public void delete(final String sessionId) {
        deleteBySessionId(sessionId);
    }

    @Override
    public AccessResult getAccessResult(final String sessionId, final String url) {
        return get(AccessResult.class, sessionId, url);
    }

    @Override
    public List<AccessResult> getAccessResultList(final String url,
            final boolean hasData) {
        final SearchResponse response = riverConfig.getClient()
                .prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.termQuery(URL, url)).execute()
                .actionGet();
        final SearchHits hits = response.getHits();
        final List<AccessResult> accessResultList = new ArrayList<AccessResult>();
        if (hits.getTotalHits() != 0) {
            for (final SearchHit searchHit : hits.getHits()) {
                accessResultList.add(Beans
                        .createAndCopy(AccessResultImpl.class,
                                searchHit.getSource())
                        .converter(new EsTimestampConverter(), timestampFields)
                        .excludesWhitespace().execute());
            }
        }
        return accessResultList;
    }

    @Override
    public void iterate(final String sessionId,
            final AccessResultCallback callback) {
        SearchResponse response = riverConfig.getClient().prepareSearch(index)
                .setTypes(type).setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(scrollTimeout))
                .setFilter(FilterBuilders.termFilter(SESSION_ID, sessionId))
                .setQuery(QueryBuilders.matchAllQuery()).setSize(scrollSize)
                .execute().actionGet();
        while (true) {
            final SearchHits searchHits = response.getHits();
            for (final SearchHit searchHit : searchHits) {
                final AccessResult accessResult = Beans
                        .createAndCopy(AccessResultImpl.class,
                                searchHit.getSource())
                        .converter(new EsTimestampConverter(), timestampFields)
                        .excludesWhitespace().execute();
                callback.iterate(accessResult);
            }

            if (searchHits.hits().length == 0) {
                break;
            }
            response = riverConfig.getClient()
                    .prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(scrollTimeout)).execute()
                    .actionGet();
        }
    }

    @Override
    public void iterateUrlDiff(final String oldSessionId,
            final String newSessionId,
            final AccessResultCallback accessResultCallback) {
        throw new UnsupportedOperationException("Unsupported.");
    }

}
