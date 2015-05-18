package org.codelibs.elasticsearch.web.config;

import java.util.Map;

import org.codelibs.elasticsearch.web.entity.ScrapingRule;

public class RiverConfig {

    private ScrapingRule scrapingRule;

    private String index;

    private String type;

    private boolean overwrite;

    private boolean incremental;

    public String getIndex() {
        return index;
    }

    public void setIndex(final String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(final boolean overwrite) {
        this.overwrite = overwrite;
    }

    public boolean isIncremental() {
        return incremental;
    }

    public void setIncremental(final boolean incremental) {
        this.incremental = incremental;
    }

    public void setScrapingRule(final Map<String, Object> settingMap, final Map<String, Object> patternMap,
            final Map<String, Map<String, Object>> scrapingRuleMap) {
        scrapingRule = new ScrapingRule(settingMap, patternMap, scrapingRuleMap);
    }

    public ScrapingRule getScrapingRule() {
        return scrapingRule;
    }

}
