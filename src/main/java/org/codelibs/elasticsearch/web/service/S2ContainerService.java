package org.codelibs.elasticsearch.web.service;

import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.elasticsearch.ElasticsearchException;
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
    protected void doStart() throws ElasticsearchException {
        logger.info("Starting S2Container...");

        final RiverConfig riverConfig = SingletonS2Container
                .getComponent(RiverConfig.class);
        riverConfig.setClient(client);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("Stopping S2Container...");

    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("Closing S2Container...");

        SingletonS2ContainerFactory.destroy();
    }

}
