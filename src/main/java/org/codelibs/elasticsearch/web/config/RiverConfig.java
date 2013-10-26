package org.codelibs.elasticsearch.web.config;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.elasticsearch.client.Client;
import org.seasar.framework.beans.BeanDesc;
import org.seasar.framework.beans.factory.BeanDescFactory;
import org.seasar.framework.util.FieldUtil;
import org.seasar.robot.entity.ResponseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiverConfig {
    private static final Logger logger = LoggerFactory
            .getLogger(RiverConfig.class);

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
            final Map<String, Object> patternMap,
            final Map<String, Map<String, Object>> scrapingRuleMap) {
        final List<ScrapingRule> ruleList = getScrapingRuleList(sessionId);
        ruleList.add(new ScrapingRule(patternMap, scrapingRuleMap));
    }

    private List<ScrapingRule> getScrapingRuleList(final String sessionId) {
        List<ScrapingRule> scrapingRuleList = sessionScrapingRuleMap
                .get(sessionId);
        if (scrapingRuleList == null) {
            scrapingRuleList = new ArrayList<RiverConfig.ScrapingRule>();
            sessionScrapingRuleMap.put(sessionId, scrapingRuleList);
        }
        return scrapingRuleList;
    }

    public Map<String, Map<String, Object>> getPropertyMapping(
            final ResponseData responseData) {
        final List<ScrapingRule> scrapingRuleList = getScrapingRuleList(responseData
                .getSessionId());
        for (final ScrapingRule scrapingRule : scrapingRuleList) {
            if (scrapingRule.matches(responseData)) {
                return scrapingRule.getRuleMap();
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

    static class ScrapingRule {
        final Map<String, Pattern> patternMap = new LinkedHashMap<String, Pattern>();

        final Map<String, Map<String, Object>> ruleMap;

        ScrapingRule(final Map<String, Object> paramPatternMap,
                final Map<String, Map<String, Object>> ruleMap) {
            this.ruleMap = ruleMap;
            for (final Map.Entry<String, Object> entry : paramPatternMap
                    .entrySet()) {
                final Object value = entry.getValue();
                if (value instanceof String) {
                    patternMap.put(entry.getKey(),
                            Pattern.compile(value.toString()));
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("patternMap: " + patternMap);
            }
        }

        boolean matches(final ResponseData responseData) {
            if (patternMap.isEmpty()) {
                return false;
            }

            try {
                final BeanDesc beanDesc = BeanDescFactory
                        .getBeanDesc(responseData.getClass());
                for (final Map.Entry<String, Pattern> entry : patternMap
                        .entrySet()) {
                    final Field field = beanDesc.getField(entry.getKey());
                    final Object value = FieldUtil.get(field, responseData);
                    if (value == null
                            || !entry.getValue().matcher(value.toString())
                                    .matches()) {
                        return false;
                    }
                }
                return true;
            } catch (final Exception e) {
                logger.warn("Invalid parameters: " + responseData, e);
                return false;
            }
        }

        public Map<String, Map<String, Object>> getRuleMap() {
            return ruleMap;
        }
    }
}
