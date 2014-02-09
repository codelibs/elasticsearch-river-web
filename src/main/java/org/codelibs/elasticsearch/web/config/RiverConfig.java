package org.codelibs.elasticsearch.web.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticsearch.client.Client;
import org.seasar.robot.entity.ResponseData;

public class RiverConfig {

    protected Client client;

    protected Map<String, List<ScrapingRule>> sessionScrapingRuleMap = new ConcurrentHashMap<String, List<ScrapingRule>>();

    protected Map<String, Map<String, Object>> riverParamMap = new ConcurrentHashMap<String, Map<String, Object>>();

    public Client getClient() {
        return client;
    }

    public void setClient(final Client client) {
        this.client = client;
    }

    public void addRiverParams(final String sessionId,
            final Map<String, Object> paramMap) {
        riverParamMap.put(sessionId, paramMap);
    }

    private Map<String, Object> getRiverParameterMap(final String sessionId) {
        return riverParamMap.get(sessionId);
    }

    public void addScrapingRule(final String sessionId,
            final Map<String, Object> settingMap,
            final Map<String, Object> patternMap,
            final Map<String, Map<String, Object>> scrapingRuleMap) {
        final List<ScrapingRule> ruleList = getScrapingRuleList(sessionId);
        ruleList.add(new ScrapingRule(settingMap, patternMap, scrapingRuleMap));
    }

    private List<ScrapingRule> getScrapingRuleList(final String sessionId) {
        List<ScrapingRule> scrapingRuleList = sessionScrapingRuleMap
                .get(sessionId);
        if (scrapingRuleList == null) {
            scrapingRuleList = new ArrayList<ScrapingRule>();
            sessionScrapingRuleMap.put(sessionId, scrapingRuleList);
        }
        return scrapingRuleList;
    }

    public ScrapingRule getScrapingRule(final ResponseData responseData) {
        final List<ScrapingRule> scrapingRuleList = getScrapingRuleList(responseData
                .getSessionId());
        for (final ScrapingRule scrapingRule : scrapingRuleList) {
            if (scrapingRule.matches(responseData)) {
                return scrapingRule;
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
        sessionScrapingRuleMap.remove(sessionId);
    }

}
