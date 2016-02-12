package org.codelibs.riverweb.config;

import java.util.HashMap;
import java.util.Map;

public class RiverConfigManager {
    protected Map<String, RiverConfig> configMap = new HashMap<>();

    public RiverConfig get(final String sessionId) {
        synchronized (configMap) {
            if (configMap.containsKey(sessionId)) {
                return configMap.get(sessionId);
            }
            RiverConfig config = new RiverConfig();
            configMap.put(sessionId, config);
            return config;
        }
    }

    public RiverConfig remove(final String sessionId) {
        synchronized (configMap) {
            return configMap.remove(sessionId);
        }
    }
}
