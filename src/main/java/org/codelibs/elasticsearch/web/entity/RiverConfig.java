package org.codelibs.elasticsearch.web.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.robot.entity.ResponseData;

public class RiverConfig {

    private String index;

    private String type;

    private boolean overwrite;

    private boolean incremental;

    private List<ScrapingRule> scrapingRuleList = new ArrayList<>();

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

    public void addScrapingRule(final Map<String, Object> settingMap, final Map<String, Object> patternMap,
            final Map<String, Map<String, Object>> scrapingRuleMap) {
        scrapingRuleList.add(new ScrapingRule(settingMap, patternMap, scrapingRuleMap));
    }

    public ScrapingRule getScrapingRule(final ResponseData responseData) {
        for (final ScrapingRule scrapingRule : scrapingRuleList) {
            if (scrapingRule.matches(responseData)) {
                return scrapingRule;
            }
        }
        return null;
    }

}
