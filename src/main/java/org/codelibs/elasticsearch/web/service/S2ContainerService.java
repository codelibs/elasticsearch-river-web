package org.codelibs.elasticsearch.web.service;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.seasar.framework.container.S2Container;
import org.seasar.framework.container.factory.S2ContainerFactory;

public class S2ContainerService extends
        AbstractLifecycleComponent<S2ContainerService> {
    private S2Container container;

    @Inject
    public S2ContainerService(final Settings settings) {
        super(settings);
        logger.info("Creating S2Container...");

        final String configPath = "app.dicon";

        container = S2ContainerFactory.create(configPath);
        container.init();

    }

    @Override
    protected void doStart() throws ElasticSearchException {
        logger.info("Starting S2Container...");

    }

    @Override
    protected void doStop() throws ElasticSearchException {
        logger.info("Stopping S2Container...");

    }

    @Override
    protected void doClose() throws ElasticSearchException {
        logger.info("Closing S2Container...");

        container.destroy();
    }

    public S2Container getContainer() {
        return container;
    }

}
