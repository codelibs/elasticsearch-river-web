package org.codelibs.elasticsearch.web.transformer;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.seasar.framework.util.ResourceUtil;
import org.seasar.robot.entity.ResponseData;
import org.seasar.robot.entity.ResultData;

public class ScrapingTransformerTest {
    @Test
    public void fess_codelibs_org() {
        ScrapingTransformer transformer = new ScrapingTransformer() {
            @Override
            protected void storeIndex(ResponseData responseData,
                    Map<String, Object> dataMap) {
                System.out.println("dataMap: " + dataMap);
            }
        };

        String sessionId = "test";
        String url = "http://fess.codelibs.org/";

        Map<String, Map<String, String>> scrapingRuleMap = new HashMap<String, Map<String, String>>();
        addScrapingRuleMap(
                scrapingRuleMap,
                "top.lead.title",
                "//DIV[contains(@class, 'top-stories-section')]//DIV[contains(@class, 'section-content')]/DIV[contains(@class, 'blended-wrapper')][1]//DIV[contains(@class, 'esc-lead-article-title-wrapper')]");
        addScrapingRuleMap(
                scrapingRuleMap,
                "top.lead.source",
                "//DIV[contains(@class, 'top-stories-section')]//DIV[contains(@class, 'section-content')]/DIV[contains(@class, 'blended-wrapper')][1]//DIV[contains(@class, 'esc-lead-article-source-wrapper')]//SPAN[contains(@class, 'al-attribution-source')]");
        addScrapingRuleMap(
                scrapingRuleMap,
                "top.news1.title",
                "//DIV[contains(@class, 'top-stories-section')]//DIV[contains(@class, 'section-content')]/DIV[contains(@class, 'blended-wrapper')][2]//DIV[contains(@class, 'esc-lead-article-title-wrapper')]");
        addScrapingRuleMap(
                scrapingRuleMap,
                "top.news1.source",
                "//DIV[contains(@class, 'top-stories-section')]//DIV[contains(@class, 'section-content')]/DIV[contains(@class, 'blended-wrapper')][2]//DIV[contains(@class, 'esc-lead-article-source-wrapper')]//SPAN[contains(@class, 'al-attribution-source')]");
        transformer.addScrapingRule(sessionId, Pattern.compile(url),
                scrapingRuleMap);
        InputStream is = null;
        try {
            is = ResourceUtil
                    .getResourceAsStream("html/fess_codelibs_org.html");
            ResponseData responseData = new ResponseData();
            responseData.setSessionId(sessionId);
            responseData.setUrl(url);
            responseData.setResponseBody(is);
            responseData.setCharSet("UTF-8");
            ResultData resultData = new ResultData();

            transformer.storeData(responseData, resultData);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private void addScrapingRuleMap(
            Map<String, Map<String, String>> scrapingRuleMap, String property,
            String path) {
        Map<String, String> valueMap = new HashMap<String, String>();
        valueMap.put("path", path);
        scrapingRuleMap.put(property, valueMap);
    }
}
