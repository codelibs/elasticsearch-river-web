package org.codelibs.elasticsearch.web.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.elasticsearch.client.Client;
import org.seasar.robot.entity.ResponseData;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RiverConfig {
    protected Client client;

    protected ObjectMapper objectMapper;

    protected Map<String, Map<Pattern, Map<String, Map<String, Object>>>> sessionPatternParamMap = new ConcurrentHashMap<String, Map<Pattern, Map<String, Map<String, Object>>>>();

    protected Map<String, Map<String, Object>> riverParamMap = new ConcurrentHashMap<String, Map<String, Object>>();

    public Client getClient() {
        return client;
    }

    public void setClient(final Client client) {
        this.client = client;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void setObjectMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void addRiverParams(final String sessionId,
            final Map<String, Object> paramMap) {
        riverParamMap.put(sessionId, paramMap);
    }

    public void addScrapingRule(final String sessionId,
            final Pattern urlPattern,
            final Map<String, Map<String, Object>> scrapingRuleMap) {
        final Map<Pattern, Map<String, Map<String, Object>>> patternParamMap = getPatternParamMap(sessionId);
        patternParamMap.put(urlPattern, scrapingRuleMap);
    }

    private Map<Pattern, Map<String, Map<String, Object>>> getPatternParamMap(
            final String sessionId) {
        Map<Pattern, Map<String, Map<String, Object>>> patternParamMap = sessionPatternParamMap
                .get(sessionId);
        if (patternParamMap == null) {
            patternParamMap = new HashMap<Pattern, Map<String, Map<String, Object>>>();
            sessionPatternParamMap.put(sessionId, patternParamMap);
        }
        return patternParamMap;
    }

    private Map<String, Object> getRiverParameterMap(final String sessionId) {
        return riverParamMap.get(sessionId);
    }

    public Map<String, Map<String, Object>> getPropertyMapping(
            final ResponseData responseData) {
        final Map<Pattern, Map<String, Map<String, Object>>> patternParamMap = getPatternParamMap(responseData
                .getSessionId());
        for (final Map.Entry<Pattern, Map<String, Map<String, Object>>> entry : patternParamMap
                .entrySet()) {
            if (entry.getKey().matcher(responseData.getUrl()).matches()) {
                return entry.getValue();
            }
        }
        return null;
    }

    public String getIndexName(final String sessionId) {
        final Map<String, Object> paramMap = getRiverParameterMap(sessionId);
        if (paramMap != null) {
            final String indexName = (String) paramMap.get("index");
            if (indexName != null) {
                return indexName;
            }
        }
        return null;
    }

    public String getTypeName(final String sessionId) {
        final Map<String, Object> paramMap = getRiverParameterMap(sessionId);
        if (paramMap != null) {
            final String indexName = (String) paramMap.get("type");
            if (indexName != null) {
                return indexName;
            }
        }
        return null;
    }

    public boolean isOverwrite(final String sessionId) {
        final Map<String, Object> paramMap = getRiverParameterMap(sessionId);
        if (paramMap != null) {
            final Boolean overwrite = (Boolean) paramMap.get("overwrite");
            return overwrite != null && overwrite.booleanValue();
        }
        return false;
    }

    public boolean isIncremental(final String sessionId) {
        final Map<String, Object> paramMap = getRiverParameterMap(sessionId);
        if (paramMap != null) {
            final Boolean overwrite = (Boolean) paramMap.get("incremental");
            return overwrite != null && overwrite.booleanValue();
        }
        return false;
    }

    public void cleanup(final String sessionId) {
        riverParamMap.remove(sessionId);
        sessionPatternParamMap.remove(sessionId);
    }

}
