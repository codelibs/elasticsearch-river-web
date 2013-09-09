package org.codelibs.elasticsearch.web.module;

import org.codelibs.elasticsearch.web.river.WebRiver;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class WebRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(WebRiver.class).asEagerSingleton();
    }
}
