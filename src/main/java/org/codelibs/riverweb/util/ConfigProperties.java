package org.codelibs.riverweb.util;

import java.util.stream.Stream;

import org.codelibs.core.misc.DynamicProperties;

public class ConfigProperties extends DynamicProperties {

    private static final long serialVersionUID = 1L;

    public ConfigProperties(String path) {
        super(path);
    }

    public String getElasticsearchClusterName(final String clusterName) {
        return clusterName == null ? getProperty("elasticsearch.cluster.name", "elasticsearch") : clusterName;
    }

    public String[] getElasticsearchHosts(String esHosts) {
        return Stream.of((esHosts == null ? getProperty("elasticsearch.hosts", "localhost") : esHosts).split(",")).map(host -> host.trim())
                .toArray(n -> new String[n]);
    }

    public String getConfigIndex() {
        return getProperty("config.index");
    }

    public String getConfigType() {
        return getProperty("config.type");
    }

    public String getQueueType() {
        return getProperty("queue.type");
    }

    public boolean isRobotsTxtEnabled() {
        return Boolean.valueOf(getProperty("robots.txt.enabled", Boolean.TRUE.toString()));
    }

}
