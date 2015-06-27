package org.codelibs.elasticsearch.web.service;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.codelibs.core.beans.util.BeanUtil;
import org.codelibs.elasticsearch.web.entity.EsAccessResult;
import org.codelibs.robot.entity.AccessResult;
import org.codelibs.robot.entity.AccessResultImpl;
import org.codelibs.robot.service.DataService;
import org.codelibs.robot.util.AccessResultCallback;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class EsDataService extends AbstractRobotService implements DataService {

    public int scrollTimeout = 60000;

    public int scrollSize = 100;

    @PostConstruct
    public void init() {
        esClient.addOnConnectListener(() -> {
            createMapping("data");
        });
    }

    @Override
    public void store(final AccessResult accessResult) {
        super.insert(accessResult, OpType.CREATE);
    }

    @Override
    public void update(final AccessResult accessResult) {
        store(accessResult);
    }

    @Override
    public void update(final List<AccessResult> accessResultList) {
        insertAll(accessResultList, OpType.INDEX);
    }

    @Override
    public int getCount(final String sessionId) {
        return (int) esClient.prepareCount(index).setTypes(sessionId).execute().actionGet().getCount();
    }

    @Override
    public void delete(final String sessionId) {
        deleteBySessionId(sessionId);
    }

    @Override
    public AccessResult getAccessResult(final String sessionId, final String url) {
        return get(AccessResultImpl.class, sessionId, url);
    }

    @Override
    public List<AccessResult> getAccessResultList(final String url, final boolean hasData) {
        final SearchResponse response =
                esClient.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.termQuery(URL, url)).execute().actionGet();
        final SearchHits hits = response.getHits();
        final List<AccessResult> accessResultList = new ArrayList<AccessResult>();
        if (hits.getTotalHits() != 0) {
            for (final SearchHit searchHit : hits.getHits()) {
                accessResultList.add(BeanUtil.copyMapToNewBean(searchHit.getSource(), EsAccessResult.class, option -> {
                    option.converter(new EsTimestampConverter(), timestampFields).excludeWhitespace();
                }));
            }
        }
        return accessResultList;
    }

    @Override
    public void iterate(final String sessionId, final AccessResultCallback callback) {
        SearchResponse response =
                esClient.prepareSearch(index)
                        .setTypes(type)
                        .setSearchType(SearchType.SCAN)
                        .setScroll(new TimeValue(scrollTimeout))
                        .setQuery(
                                QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.termFilter(SESSION_ID, sessionId)))
                        .setSize(scrollSize).execute().actionGet();
        while (true) {
            final SearchHits searchHits = response.getHits();
            for (final SearchHit searchHit : searchHits) {
                final AccessResult accessResult = BeanUtil.copyMapToNewBean(searchHit.getSource(), EsAccessResult.class, option -> {
                    option.converter(new EsTimestampConverter(), timestampFields).excludeWhitespace();
                });
                callback.iterate(accessResult);
            }

            if (searchHits.hits().length == 0) {
                break;
            }
            response = esClient.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(scrollTimeout)).execute().actionGet();
        }
    }
}
