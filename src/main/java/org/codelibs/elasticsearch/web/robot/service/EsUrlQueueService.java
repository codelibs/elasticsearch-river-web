package org.codelibs.elasticsearch.web.robot.service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;

import javax.annotation.Resource;

import org.codelibs.elasticsearch.web.robot.entity.EsUrlQueue;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.seasar.framework.util.StringUtil;
import org.seasar.robot.Constants;
import org.seasar.robot.entity.AccessResult;
import org.seasar.robot.entity.UrlQueue;
import org.seasar.robot.service.UrlQueueService;
import org.seasar.robot.util.AccessResultCallback;

public class EsUrlQueueService extends AbstractRobotService implements
        UrlQueueService {
    private static final ESLogger logger = Loggers
            .getLogger(EsUrlQueueService.class);

    @Resource
    protected EsDataService dataService;

    protected Queue<UrlQueue> crawlingUrlQueue = new ConcurrentLinkedQueue<UrlQueue>();

    public int pollingFetchSize = 20;

    public int maxCrawlingQueueSize = 100;

    @Override
    public void updateSessionId(final String oldSessionId,
            final String newSessionId) {
        // TODO Script Query
    }

    @Override
    public void add(final String sessionId, final String url) {
        final UrlQueue urlQueue = new EsUrlQueue();
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
        super.insert(urlQueue, OpType.CREATE);
    }

    @Override
    public void delete(final String sessionId) {
        deleteBySessionId(sessionId);
    }

    @Override
    public void offerAll(final String sessionId,
            final List<UrlQueue> urlQueueList) {
        if (logger.isDebugEnabled()) {
            logger.debug("Offering URL: Session ID: {}, UrlQueue: {}",
                    sessionId, urlQueueList);
        }
        final List<UrlQueue> targetList = new ArrayList<UrlQueue>(
                urlQueueList.size());
        for (final UrlQueue urlQueue : urlQueueList) {
            if (!exists(sessionId, urlQueue.getUrl())
                    && !dataService.exists(sessionId, urlQueue.getUrl())) {
                urlQueue.setSessionId(sessionId);
                targetList.add(urlQueue);
            } else if (logger.isDebugEnabled()) {
                logger.debug("Existed URL: Session ID: {}, UrlQueue: {}",
                        sessionId, urlQueue);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Offered URL: Session ID: {}, UrlQueue: {}",
                    sessionId, targetList);
        }
        if (!targetList.isEmpty()) {
            insertAll(targetList, OpType.CREATE);
        }
    }

    @Override
    public UrlQueue poll(final String sessionId) {
        final Lock lock = riverConfig.getLock(sessionId);
        if (lock == null) {
            return null;
        }

        try {
            lock.lock();
            final List<EsUrlQueue> urlQueueList = getList(EsUrlQueue.class,
                    sessionId, null, 0, pollingFetchSize, SortBuilders
                            .fieldSort(CREATE_TIME).order(SortOrder.ASC));
            if (urlQueueList.isEmpty()) {
                return null;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Queued URL: {}", urlQueueList);
            }
            final Client client = riverConfig.getClient();
            for (final EsUrlQueue urlQueue : urlQueueList) {
                final String url = urlQueue.getUrl();
                if (exists(sessionId, url)) {
                    crawlingUrlQueue.add(urlQueue);
                    if (crawlingUrlQueue.size() > maxCrawlingQueueSize) {
                        crawlingUrlQueue.poll();
                    }
                    super.delete(sessionId, url);
                    if (riverConfig.isIncremental(sessionId)) {
                        updateLastModified(sessionId, client, urlQueue, url);
                    }
                    return urlQueue;
                } else if (logger.isDebugEnabled()) {
                    logger.debug("Already Deleted: {}", urlQueue);
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    private void updateLastModified(final String sessionId,
            final Client client, final EsUrlQueue urlQueue, final String url) {
        try {
            final SearchResponse response = client
                    .prepareSearch(riverConfig.getIndexName(sessionId))
                    .setQuery(QueryBuilders.termQuery("url", url))
                    .addSort(
                            SortBuilders.fieldSort(LAST_MODIFIED).order(
                                    SortOrder.DESC)).setFrom(0).setSize(1)
                    .execute().actionGet();
            final SearchHits hits = response.getHits();
            if (hits.getTotalHits() > 0) {
                final SearchHit hit = hits.getHits()[0];
                final Map<String, Object> sourceMap = hit.getSource();
                final Date date = (Date) sourceMap.get(LAST_MODIFIED);
                if (date != null) {
                    urlQueue.setLastModified(new Timestamp(date.getTime()));
                }
            }
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to update a last modified: " + sessionId,
                        e);
            }
        }
    }

    @Override
    public void saveSession(final String sessionId) {
        // TODO use cache
    }

    @Override
    public boolean visited(final UrlQueue urlQueue) {
        final String url = urlQueue.getUrl();
        if (StringUtil.isBlank(url)) {
            if (logger.isDebugEnabled()) {
                logger.debug("URL is a blank: " + url);
            }
            return false;
        }

        final String sessionId = urlQueue.getSessionId();
        if (super.exists(sessionId, url)) {
            return true;
        }

        final AccessResult accessResult = dataService.getAccessResult(
                sessionId, url);
        if (accessResult != null) {
            return true;
        }

        return false;
    }

    @Override
    protected boolean exists(final String sessionId, final String url) {
        final boolean ret = super.exists(sessionId, url);
        if (!ret) {
            for (final UrlQueue urlQueue : crawlingUrlQueue) {
                if (sessionId.equals(urlQueue.getSessionId())
                        && url.equals(urlQueue.getUrl())) {
                    return true;
                }
            }
        }
        return ret;
    }

    @Override
    public void generateUrlQueues(final String previousSessionId,
            final String sessionId) {
        dataService.iterate(previousSessionId, new AccessResultCallback() {
            @Override
            public void iterate(final AccessResult accessResult) {
                final UrlQueue urlQueue = new EsUrlQueue();
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
