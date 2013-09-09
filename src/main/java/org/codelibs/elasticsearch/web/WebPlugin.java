package org.codelibs.elasticsearch.web;

import java.util.Collection;

import org.codelibs.elasticsearch.web.module.ScheduleModule;
import org.codelibs.elasticsearch.web.module.WebRiverModule;
import org.codelibs.elasticsearch.web.service.ScheduleService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class WebPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "WebPlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-river-web plugin.";
    }

    // for River
    public void onModule(final RiversModule module) {
        module.registerRiver("web", WebRiverModule.class);
    }

    // for Service
    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(ScheduleModule.class);
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists
                .newArrayList();
        services.add(ScheduleService.class);
        return services;
    }
}
