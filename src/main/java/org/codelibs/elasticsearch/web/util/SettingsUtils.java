package org.codelibs.elasticsearch.web.util;

import java.util.Map;

public final class SettingsUtils {
    private SettingsUtils() {
    }

    public static <T, V> T get(final Map<String, V> settings, final String key) {
        return get(settings, key, null);
    }

    @SuppressWarnings("unchecked")
    public static <T, V> T get(final Map<String, V> settings, final String key, final T defaultValue) {
        if (settings != null) {
            final V value = settings.get(key);
            if (value instanceof Number) {
                if (defaultValue instanceof Integer) {
                    return (T) Integer.valueOf(((Number) value).intValue());
                } else if (defaultValue instanceof Long) {
                    return (T) Long.valueOf(((Number) value).longValue());
                } else if (defaultValue instanceof Float) {
                    return (T) Float.valueOf(((Number) value).floatValue());
                } else if (defaultValue instanceof Double) {
                    return (T) Double.valueOf(((Number) value).doubleValue());
                } else {
                    return (T) value;
                }
            } else if (value != null) {
                return (T) value;
            }
        }
        return defaultValue;
    }
}
