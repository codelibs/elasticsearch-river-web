package org.codelibs.elasticsearch.web.util;

import java.util.Map;

public class ParameterUtil {
    private ParameterUtil() {
    }

    @SuppressWarnings("unchecked")
    public static <T, V> T getValue(final Map<String, V> settings,
            final String key, final T defaultValue) {
        final V value = settings.get(key);
        if (value != null) {
            return (T) value;
        }
        return defaultValue;
    }
}
