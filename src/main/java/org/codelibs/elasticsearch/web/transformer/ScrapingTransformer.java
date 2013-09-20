package org.codelibs.elasticsearch.web.transformer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.codelibs.elasticsearch.web.util.IdUtil;
import org.codelibs.elasticsearch.web.util.ParameterUtil;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.seasar.framework.beans.BeanDesc;
import org.seasar.framework.beans.factory.BeanDescFactory;
import org.seasar.framework.beans.util.Beans;
import org.seasar.framework.container.SingletonS2Container;
import org.seasar.framework.container.annotation.tiger.InitMethod;
import org.seasar.framework.util.MethodUtil;
import org.seasar.framework.util.StringUtil;
import org.seasar.robot.RobotCrawlAccessException;
import org.seasar.robot.entity.AccessResultData;
import org.seasar.robot.entity.ResponseData;
import org.seasar.robot.entity.ResultData;
import org.seasar.robot.transformer.impl.XpathTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScrapingTransformer extends
        org.seasar.robot.transformer.impl.HtmlTransformer {

    private static final Logger logger = LoggerFactory
            .getLogger(XpathTransformer.class);

    private static final String[] queryTypes = new String[] { "className",
            "data", "html", "id", "ownText", "tagName", "text", "val" };

    public String[] copiedResonseDataFields = new String[] { "url",
            "parentUrl", "httpStatusCode", "method", "charSet",
            "contentLength", "mimeType", "executionTime", "lastModified" };

    protected RiverConfig riverConfig;

    @InitMethod
    public void init() {
        riverConfig = SingletonS2Container.getComponent(RiverConfig.class);
    }

    @Override
    protected void storeData(final ResponseData responseData,
            final ResultData resultData) {
        final Map<String, Map<String, Object>> scrapingRuleMap = riverConfig
                .getPropertyMapping(responseData);
        if (scrapingRuleMap == null) {
            return;
        }

        org.jsoup.nodes.Document document = null;
        String charsetName = responseData.getCharSet();
        if (charsetName == null) {
            charsetName = "UTF-8";
        }
        try {
            document = Jsoup.parse(responseData.getResponseBody(), charsetName,
                    responseData.getUrl());
        } catch (final IOException e) {
            throw new RobotCrawlAccessException("Could not parse "
                    + responseData.getUrl(), e);
        }

        final Map<String, Object> dataMap = new HashMap<String, Object>();
        Beans.copy(responseData, dataMap).includes(copiedResonseDataFields)
                .excludesNull().excludesWhitespace().execute();
        for (final Map.Entry<String, Map<String, Object>> entry : scrapingRuleMap
                .entrySet()) {
            final Map<String, Object> params = entry.getValue();
            final boolean isTrimSpaces = ParameterUtil.getValue(params,
                    "trimSpaces", Boolean.FALSE).booleanValue();
            final boolean isArray = ParameterUtil.getValue(params,
                    "trimSpaces", Boolean.FALSE).booleanValue();

            final List<String> strList = new ArrayList<String>();
            final BeanDesc elementDesc = BeanDescFactory
                    .getBeanDesc(Element.class);

            for (final String queryType : queryTypes) {
                final String query = ParameterUtil.getValue(params, queryType,
                        null);
                if (StringUtil.isNotBlank(query)) {
                    final Element[] elements = getElements(
                            new Element[] { document }, query);
                    for (Element element : elements) {
                        final Method queryMethod = elementDesc
                                .getMethod(queryType);
                        strList.add(trimSpaces((String) MethodUtil.invoke(
                                queryMethod, element, new Object[0]),
                                isTrimSpaces));
                    }
                    break;
                }
            }

            addPropertyData(dataMap, entry.getKey(), isArray ? strList
                    : StringUtils.join(strList, " "));
        }

        storeIndex(responseData, dataMap);
    }

    protected Element[] getElements(Element[] elements, String query) {
        Element[] targets = elements;
        Pattern pattern = Pattern
                .compile(":eq\\(([0-9]+)\\)|:lt\\(([0-9]+)\\)|:gt\\(([0-9]+)\\)");
        Matcher matcher = pattern.matcher(query);
        StringBuffer buf = new StringBuffer();
        while (matcher.find()) {
            String value = matcher.group();
            matcher.appendReplacement(buf, "");
            if (buf.charAt(buf.length() - 1) != ' ') {
                try {
                    int index = Integer.parseInt(matcher.group(1));
                    List<Element> elementList = new ArrayList<Element>();
                    String childQuery = buf.toString();
                    for (Element element : targets) {
                        Elements childElements = element.select(childQuery);
                        if (value.startsWith(":eq")) {
                            if (index < childElements.size()) {
                                elementList.add(childElements.get(index));
                            }
                        } else if (value.startsWith(":lt")) {
                            for (int i = 0; i < childElements.size()
                                    && i < index; i++) {
                                elementList.add(childElements.get(i));
                            }
                        } else if (value.startsWith(":gt")) {
                            for (int i = index + 1; i < childElements.size(); i++) {
                                elementList.add(childElements.get(i));
                            }
                        }
                    }
                    targets = elementList.toArray(new Element[elementList
                            .size()]);
                    buf.setLength(0);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid number: " + query, e);
                    buf.append(value);
                }
            } else {
                buf.append(value);
            }
        }
        matcher.appendTail(buf);
        String lastQuery = buf.toString();
        if (StringUtil.isNotBlank(lastQuery)) {
            List<Element> elementList = new ArrayList<Element>();
            for (Element element : targets) {
                Elements childElements = element.select(lastQuery);
                for (int i = 0; i < childElements.size(); i++) {
                    elementList.add(childElements.get(i));
                }
            }
            targets = elementList.toArray(new Element[elementList.size()]);
        }
        return targets;
    }

    protected String trimSpaces(final String value, final boolean trimSpaces) {
        if (value == null) {
            return null;
        }
        if (trimSpaces) {
            return value.replaceAll("\\s+", " ").trim();
        }
        return value;
    }

    protected void addPropertyData(final Map<String, Object> dataMap,
            final String key, final Object value) {
        Map<String, Object> currentDataMap = dataMap;
        final String[] keys = key.split("\\.");
        for (int i = 0; i < keys.length - 1; i++) {
            final String currentKey = keys[i];
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) currentDataMap
                    .get(currentKey);
            if (map == null) {
                map = new HashMap<String, Object>();
                currentDataMap.put(currentKey, map);
            }
            currentDataMap = map;
        }
        currentDataMap.put(keys[keys.length - 1], value);
    }

    protected void storeIndex(final ResponseData responseData,
            final Map<String, Object> dataMap) {
        final String id = IdUtil.getId(responseData.getUrl());
        final String sessionId = responseData.getSessionId();
        final String indexName = riverConfig.getIndexName(sessionId);
        final boolean overwrite = riverConfig.isOverwrite(sessionId);
        final Client client = riverConfig.getClient();

        if (logger.isDebugEnabled()) {
            logger.debug("Index: " + indexName + ", sessionId: " + sessionId
                    + ", Data: " + dataMap);
        }

        if (overwrite) {
            client.prepareDeleteByQuery(indexName)
                    .setQuery(
                            QueryBuilders.termQuery("url",
                                    responseData.getUrl())).execute()
                    .actionGet();
            client.admin().indices().prepareRefresh(indexName).execute()
                    .actionGet();
        }
        try {
            final String content = riverConfig.getObjectMapper()
                    .writeValueAsString(dataMap);
            client.prepareIndex(indexName, sessionId, id).setRefresh(true)
                    .setSource(content).execute().actionGet();
        } catch (final Exception e) {
            logger.warn("Could not write a content into index.", e);
        }
    }

    /**
     * Returns data as XML content of String.
     * 
     * @return XML content of String.
     */
    @Override
    public Object getData(final AccessResultData accessResultData) {
        return null;
    }

}
