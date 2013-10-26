package org.codelibs.elasticsearch.web.robot.transformer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.junit.Test;
import org.seasar.framework.util.ResourceUtil;
import org.seasar.robot.entity.ResponseData;
import org.seasar.robot.entity.ResultData;

public class ScrapingTransformerTest {
    @Test
    public void fess_codelibs_org() {
        RiverConfig riverConfig = new RiverConfig();
        ScrapingTransformer transformer = new ScrapingTransformer() {
            @SuppressWarnings("unchecked")
            @Override
            protected void storeIndex(ResponseData responseData,
                    Map<String, Object> dataMap) {
                System.out.println(dataMap);
                assertThat(
                        ((List<String>) ((Map<String, Object>) dataMap.get("nav"))
                                .get("sideMenus")).size(), is(27));
                assertThat(
                        ((Map<String, Object>) dataMap.get("section1")).get(
                                "title").toString(), is("What is Fess?"));
                assertThat(
                        ((List<String>) ((Map<String, Object>) dataMap.get("section1"))
                                .get("body")).size(), is(2));
                assertThat(
                        ((Map<String, Object>) dataMap.get("section2")).get(
                                "title").toString(), is("Features"));
                assertThat(
                        ((List<String>) ((Map<String, Object>) dataMap.get("section2"))
                                .get("body")).size(), is(12));
            }
        };
        transformer.riverConfig = riverConfig;

        String sessionId = "test";
        String url = "http://fess.codelibs.org/";

        Map<String, Map<String, Object>> scrapingRuleMap = new HashMap<String, Map<String, Object>>();
        addScrapingRuleMap(scrapingRuleMap, "text", "nav.sideMenus",
                "div.sidebar-nav ul li", Boolean.TRUE, Boolean.TRUE);
        addScrapingRuleMap(scrapingRuleMap, "text", "section1.title",
                "div.section:eq(0) h2", null, null);
        addScrapingRuleMap(scrapingRuleMap, "text", "section1.body",
                "div.section:eq(0) p", Boolean.TRUE, Boolean.TRUE);
        addScrapingRuleMap(scrapingRuleMap, "text", "section2.title",
                "div.section:eq(1) h2", null, null);
        addScrapingRuleMap(scrapingRuleMap, "text", "section2.body",
                "div.section:eq(1) ul li", Boolean.TRUE, Boolean.TRUE);
        Map<String, Object> patternMap = new HashMap<String, Object>();
        patternMap.put("url", url);
        riverConfig.addScrapingRule(sessionId, patternMap, scrapingRuleMap);
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
            Map<String, Map<String, Object>> scrapingRuleMap, String type,
            String property, String path, Boolean isArray, Boolean trimSpaces) {
        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(type, path);
        if (isArray != null) {
            valueMap.put("isArray", isArray);
        }
        if (trimSpaces != null) {
            valueMap.put("trimSpaces", trimSpaces);
        }
        scrapingRuleMap.put(property, valueMap);
    }
}
