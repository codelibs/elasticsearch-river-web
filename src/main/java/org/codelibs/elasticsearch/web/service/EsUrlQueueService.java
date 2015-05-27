package org.codelibs.elasticsearch.web.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.elasticsearch.web.entity.EsUrlQueue;
import org.codelibs.robot.Constants;
import org.codelibs.robot.entity.AccessResult;
import org.codelibs.robot.entity.UrlQueue;
import org.codelibs.robot.service.UrlQueueService;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsUrlQueueService extends AbstractRobotService implements UrlQueueService {
    private static final Logger logger = LoggerFactory.getLogger(EsUrlQueueService.class);

    @Resource
    protected EsDataService dataService;

    protected Queue<UrlQueue> crawlingUrlQueue = new ConcurrentLinkedQueue<UrlQueue>();

    public int pollingFetchSize = 20;

    public int maxCrawlingQueueSize = 100;

    @PostConstruct
    public void init() {
        esClient.addOnConnectListener(() -> {
            createMapping("queue");
        });
    }

    @Override
    public void updateSessionId(final String oldSessionId, final String newSessionId) {
        // TODO Script Query
    }

    @Override
    public void add(final String sessionId, final String url) {
        final UrlQueue urlQueue = new EsUrlQueue();
        urlQueue.setSessionId(sessionId);
        urlQueue.setUrl(url);
        urlQueue.setCreateTime(System.currentTimeMillis());
        urlQueue.setLastModified(0L);
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
    public void offerAll(final String sessionId, final List<UrlQueue> urlQueueList) {
        if (logger.isDebugEnabled()) {
            logger.debug("Offering URL: Session ID: {}, UrlQueue: {}", sessionId, urlQueueList);
        }
        final List<UrlQueue> targetList = new ArrayList<UrlQueue>(urlQueueList.size());
        for (final UrlQueue urlQueue : urlQueueList) {
            if (!exists(sessionId, urlQueue.getUrl()) && !dataService.exists(sessionId, urlQueue.getUrl())) {
                urlQueue.setSessionId(sessionId);
                targetList.add(urlQueue);
            } else if (logger.isDebugEnabled()) {
                logger.debug("Existed URL: Session ID: {}, UrlQueue: {}", sessionId, urlQueue);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Offered URL: Session ID: {}, UrlQueue: {}", sessionId, targetList);
        }
        if (!targetList.isEmpty()) {
            insertAll(targetList, OpType.CREATE);
        }
    }

    @Override
    public UrlQueue poll(final String sessionId) {
        final List<EsUrlQueue> urlQueueList =
                getList(EsUrlQueue.class, sessionId, null, 0, pollingFetchSize, SortBuilders.fieldSort(CREATE_TIME).order(SortOrder.ASC));
        if (urlQueueList.isEmpty()) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Queued URL: {}", urlQueueList);
        }
        final Client client = esClient;
        for (final EsUrlQueue urlQueue : urlQueueList) {
            final String url = urlQueue.getUrl();
            if (crawlingUrlQueue.size() > maxCrawlingQueueSize) {
                crawlingUrlQueue.poll();
            }
            if (super.delete(sessionId, url)) {
                crawlingUrlQueue.add(urlQueue);
                if (riverConfig.isIncremental()) {
                    updateLastModified(sessionId, client, urlQueue, url);
                }
                return urlQueue;
            } else if (logger.isDebugEnabled()) {
                logger.debug("Already Deleted: {}", urlQueue);
            }
        }
        return null;
    }

    private void updateLastModified(final String sessionId, final Client client, final EsUrlQueue urlQueue, final String url) {
        try {
            final SearchResponse response =
                    client.prepareSearch(riverConfig.getIndex()).setQuery(QueryBuilders.termQuery("url", url))
                            .addSort(SortBuilders.fieldSort(LAST_MODIFIED).order(SortOrder.DESC)).setFrom(0).setSize(1).execute()
                            .actionGet();
            final SearchHits hits = response.getHits();
            if (hits.getTotalHits() > 0) {
                final SearchHit hit = hits.getHits()[0];
                final Map<String, Object> sourceMap = hit.getSource();
                final Date date = getDateFromSource(sourceMap, LAST_MODIFIED);
                if (date != null) {
                    urlQueue.setLastModified(date.getTime());
                }
            }
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to update a last modified: " + sessionId, e);
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

        final AccessResult accessResult = dataService.getAccessResult(sessionId, url);
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
                if (sessionId.equals(urlQueue.getSessionId()) && url.equals(urlQueue.getUrl())) {
                    return true;
                }
            }
        }
        return ret;
    }

    @Override
    public void generateUrlQueues(final String previousSessionId, final String sessionId) {
        dataService.iterate(previousSessionId, accessResult -> {
            final UrlQueue urlQueue = new EsUrlQueue();
            urlQueue.setSessionId(sessionId);
            urlQueue.setMethod(accessResult.getMethod());
            urlQueue.setUrl(accessResult.getUrl());
            urlQueue.setParentUrl(accessResult.getParentUrl());
            urlQueue.setDepth(0);
            urlQueue.setLastModified(accessResult.getLastModified());
            urlQueue.setCreateTime(System.currentTimeMillis());
            insert(urlQueue);
        });
    }

}
