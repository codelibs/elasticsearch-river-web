package org.codelibs.elasticsearch.web.module;

import org.codelibs.elasticsearch.web.service.ScheduleService;
import org.elasticsearch.common.inject.AbstractModule;

public class ScheduleModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ScheduleService.class).asEagerSingleton();
    }
}