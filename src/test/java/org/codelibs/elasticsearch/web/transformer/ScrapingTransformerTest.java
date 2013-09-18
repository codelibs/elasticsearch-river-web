package org.codelibs.elasticsearch.web.transformer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
                assertThat(
                        ((List<String>) ((Map<String, Object>) dataMap.get("nav"))
                                .get("sideMenus")).size(), is(14));
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
        addScrapingRuleMap(scrapingRuleMap, "nav.sideMenus",
                "//DIV[contains(@class, 'sidebar-nav')]/UL/LI", Boolean.TRUE,
                Boolean.TRUE);
        addScrapingRuleMap(scrapingRuleMap, "section1.title",
                "//DIV[@class='section'][1]/H2", null, null);
        addScrapingRuleMap(scrapingRuleMap, "section1.body",
                "//DIV[@class='section'][1]/P", Boolean.TRUE, Boolean.TRUE);
        addScrapingRuleMap(scrapingRuleMap, "section2.title",
                "//DIV[@class='section'][2]/H2", null, null);
        addScrapingRuleMap(scrapingRuleMap, "section2.body",
                "//DIV[@class='section'][2]//LI", Boolean.TRUE, Boolean.TRUE);
        riverConfig.addScrapingRule(sessionId, Pattern.compile(url),
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
            Map<String, Map<String, Object>> scrapingRuleMap, String property,
            String path, Boolean isArray, Boolean trimSpaces) {
        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put("path", path);
        if (isArray != null) {
            valueMap.put("isArray", isArray);
        }
        if (trimSpaces != null) {
            valueMap.put("trimSpaces", trimSpaces);
        }
        scrapingRuleMap.put(property, valueMap);
    }
}
