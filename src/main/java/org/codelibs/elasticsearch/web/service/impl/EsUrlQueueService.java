package org.codelibs.elasticsearch.web.service.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.codelibs.elasticsearch.web.util.IdUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.seasar.robot.Constants;
import org.seasar.robot.entity.AccessResult;
import org.seasar.robot.entity.UrlQueue;
import org.seasar.robot.entity.UrlQueueImpl;
import org.seasar.robot.service.UrlQueueService;
import org.seasar.robot.util.AccessResultCallback;

public class EsUrlQueueService extends AbstractRobotService implements
        UrlQueueService {

    @Resource
    protected EsDataService dataService;

    public int pollingFetchSize = 20;

    @Override
    public void updateSessionId(final String oldSessionId,
            final String newSessionId) {
        // TODO Script Query
    }

    @Override
    public void add(final String sessionId, final String url) {
        final UrlQueue urlQueue = new UrlQueueImpl();
        urlQueue.setSessionId(sessionId);
        urlQueue.setUrl(url);
        urlQueue.setCreateTime(new Timestamp(System.currentTimeMillis()));
        urlQueue.setLastModified(new Timestamp(0));
        urlQueue.setDepth(0);
        urlQueue.setMethod(Constants.GET_METHOD);
        insert(urlQueue);
    }

    @Override
    public void insert(final UrlQueue urlQueue) {
        super.insert(urlQueue);
    }

    @Override
    public void delete(final String sessionId) {
        deleteBySessionId(sessionId);
    }

    @Override
    public void offerAll(final String sessionId,
            final List<UrlQueue> urlQueueList) {
        final List<UrlQueue> targetList = new ArrayList<UrlQueue>(
                urlQueueList.size());
        for (final UrlQueue urlQueue : urlQueueList) {
            final String id = IdUtil.getId(urlQueue.getUrl());
            if (!exists(sessionId, id) && !dataService.exists(sessionId, id)) {
                urlQueue.setSessionId(sessionId);
                targetList.add(urlQueue);
            }
        }
        if (!targetList.isEmpty()) {
            insertAll(targetList);
        }
    }

    @Override
    public UrlQueue poll(final String sessionId) {
        final List<UrlQueueImpl> urlQueueList = getList(UrlQueueImpl.class,
                sessionId, null, 0, pollingFetchSize,
                SortBuilders.fieldSort("createTime").order(SortOrder.ASC));
        if (urlQueueList.isEmpty()) {
            return null;
        }
        final Client client = riverConfig.getClient();
        for (final UrlQueueImpl urlQueue : urlQueueList) {
            synchronized (client) {
                final String id = IdUtil.getId(urlQueue);
                if (exists(sessionId, id)) {
                    super.delete(sessionId, id);
                    if (riverConfig.isIncremental(sessionId)) {
                        final SearchResponse response = client
                                .prepareSearch(
                                        riverConfig.getIndexName(sessionId))
                                .setQuery(
                                        QueryBuilders.termQuery("url",
                                                urlQueue.getUrl()))
                                .addSort(
                                        SortBuilders.fieldSort("lastModified")
                                                .order(SortOrder.DESC))
                                .setFrom(0).setSize(1).execute().actionGet();
                        final SearchHits hits = response.getHits();
                        if (hits.getTotalHits() > 0) {
                            final SearchHit hit = hits.getHits()[0];
                            final Map<String, Object> sourceMap = hit
                                    .getSource();
                            final Date date = (Date) sourceMap
                                    .get("lastModified");
                            if (date != null) {
                                urlQueue.setLastModified(new Timestamp(date
                                        .getTime()));
                            }
                        }
                    }
                    return urlQueue;
                }
            }
        }
        return null;
    }

    @Override
    public void saveSession(final String sessionId) {
        // TODO use cache
    }

    @Override
    public boolean visited(final UrlQueue urlQueue) {
        return exists(urlQueue.getSessionId(), IdUtil.getId(urlQueue));
    }

    @Override
    public void generateUrlQueues(final String previousSessionId,
            final String sessionId) {
        dataService.iterate(previousSessionId, new AccessResultCallback() {
            @Override
            public void iterate(final AccessResult accessResult) {
                final UrlQueue urlQueue = new UrlQueueImpl();
                urlQueue.setSessionId(sessionId);
                urlQueue.setMethod(accessResult.getMethod());
                urlQueue.setUrl(accessResult.getUrl());
                urlQueue.setParentUrl(accessResult.getParentUrl());
                urlQueue.setDepth(0);
                urlQueue.setLastModified(accessResult.getLastModified());
                urlQueue.setCreateTime(new Timestamp(System.currentTimeMillis()));
                insert(urlQueue);
            }
        });
    }

}
