package org.codelibs.elasticsearch.web.service;

import org.codelibs.elasticsearch.web.service.impl.EsDataService;
import org.codelibs.elasticsearch.web.service.impl.EsUrlFilterService;
import org.codelibs.elasticsearch.web.service.impl.EsUrlQueueService;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.seasar.framework.container.SingletonS2Container;
import org.seasar.framework.container.factory.SingletonS2ContainerFactory;

public class S2ContainerService extends
        AbstractLifecycleComponent<S2ContainerService> {
    private Client client;

    @Inject
    public S2ContainerService(final Settings settings, final Client client) {
        super(settings);
        this.client = client;

        logger.info("Creating S2Container...");

        SingletonS2ContainerFactory.init();
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        logger.info("Starting S2Container...");

        final EsDataService dataService = SingletonS2Container
                .getComponent(EsDataService.class);
        dataService.setClient(client);
        final EsUrlQueueService urlQueueService = SingletonS2Container
                .getComponent(EsUrlQueueService.class);
        urlQueueService.setClient(client);
        final EsUrlFilterService urlFilterService = SingletonS2Container
                .getComponent(EsUrlFilterService.class);
        urlFilterService.setClient(client);
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        logger.info("Stopping S2Container...");

    }

    @Override
    protected void doClose() throws ElasticSearchException {
        logger.info("Closing S2Container...");

        SingletonS2ContainerFactory.destroy();
    }

}
