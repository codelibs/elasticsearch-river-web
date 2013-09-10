package org.codelibs.elasticsearch.web.module;

import org.codelibs.elasticsearch.web.service.S2ContainerService;
import org.elasticsearch.common.inject.AbstractModule;

public class S2ContainerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(S2ContainerService.class).asEagerSingleton();
    }
}