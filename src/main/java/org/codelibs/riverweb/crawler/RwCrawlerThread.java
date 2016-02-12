package org.codelibs.riverweb.crawler;

import java.util.Date;

import org.codelibs.fess.crawler.CrawlerThread;
import org.codelibs.fess.crawler.client.CrawlerClient;
import org.codelibs.fess.crawler.client.EsClient;
import org.codelibs.fess.crawler.entity.UrlQueue;
import org.codelibs.riverweb.config.RiverConfig;
import org.codelibs.riverweb.config.RiverConfigManager;
import org.codelibs.riverweb.util.ConversionUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.lastaflute.di.core.SingletonLaContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RwCrawlerThread extends CrawlerThread {
    private static final Logger logger = LoggerFactory.getLogger(RwCrawlerThread.class);

    @Override
    protected boolean isContentUpdated(final CrawlerClient client, final UrlQueue<?> urlQueue) {
        final RiverConfigManager riverConfigManager = SingletonLaContainer.getComponent(RiverConfigManager.class);
        final RiverConfig riverConfig = riverConfigManager.get(crawlerContext.getSessionId());
        if (riverConfig.isIncremental()) {
            final EsClient esClient = SingletonLaContainer.getComponent(EsClient.class);
            try {
                final SearchResponse response = esClient.prepareSearch(riverConfig.getIndex()).setTypes(riverConfig.getType())
                        .setQuery(QueryBuilders.termQuery("url", urlQueue.getUrl())).addField("lastModified")
                        .addSort("lastModified", SortOrder.DESC).execute().actionGet();
                final SearchHits hits = response.getHits();
                if (hits.getTotalHits() > 0) {
                    final SearchHitField lastModifiedField = hits.getAt(0).getFields().get("lastModified");
                    if (lastModifiedField != null) {
                        final Date lastModified = ConversionUtil.convert(lastModifiedField.getValue(), Date.class);
                        if (lastModified != null) {
                            urlQueue.setLastModified(lastModified.getTime());
                        }
                    }
                }
            } catch (final Exception e) {
                logger.debug("Failed to retrieve lastModified.", e);
            }
        }
        return super.isContentUpdated(client, urlQueue);
    }
}
