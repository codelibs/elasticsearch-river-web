package org.codelibs.elasticsearch.web.robot.transformer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.codelibs.elasticsearch.web.config.RiverConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScrapingTransformer extends
        org.seasar.robot.transformer.impl.HtmlTransformer {

    private static final String VALUE_QUERY_TYPE = "value";

    private static final String ARGS_QUERY_TYPE = "args";

    private static final String IS_ARRAY_PROP_NAME = "isArray";

    private static final String TRIM_SPACES_PROP_NAME = "trimSpaces";

    private static final String TIMESTAMP_FIELD = "@timestamp";

    private static final String POSITION_FIELD = "position";

    private static final String ARRAY_PROPERTY_PREFIX = "[]";

    private static final Logger logger = LoggerFactory
            .getLogger(ScrapingTransformer.class);

    private static final String[] queryTypes = new String[] { "className",
            "data", "html", "id", "ownText", "tagName", "text", "val",
            "nodeName", "outerHtml", "attr", "baseUri", "absUrl" };

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
            logger.info("No scraping rule.");
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

        final Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
        Beans.copy(responseData, dataMap).includes(copiedResonseDataFields)
                .excludesNull().excludesWhitespace().execute();
        if (logger.isDebugEnabled()) {
            logger.debug("ruleMap: " + scrapingRuleMap);
            logger.debug("dataMap: " + dataMap);
        }
        for (final Map.Entry<String, Map<String, Object>> entry : scrapingRuleMap
                .entrySet()) {
            final String propName = entry.getKey();
            final Map<String, Object> params = entry.getValue();
            final boolean isTrimSpaces = ParameterUtil.getValue(params,
                    TRIM_SPACES_PROP_NAME, Boolean.FALSE).booleanValue();
            final boolean isArray = ParameterUtil.getValue(params,
                    IS_ARRAY_PROP_NAME, Boolean.FALSE).booleanValue();

            final List<String> strList = new ArrayList<String>();

            final String value = ParameterUtil.getValue(params,
                    VALUE_QUERY_TYPE, null);
            if (StringUtil.isNotBlank(value)) {
                strList.add(trimSpaces(value, isTrimSpaces));
            } else {
                for (final String queryType : queryTypes) {
                    final Object queryObj = ParameterUtil.getValue(params,
                            queryType, null);
                    Element[] elements = null;
                    if (queryObj instanceof String) {
                        elements = getElements(new Element[] { document },
                                queryObj.toString());
                    } else if (queryObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        final List<String> queryList = (List<String>) queryObj;
                        elements = getElements(new Element[] { document },
                                queryList,
                                propName.startsWith(ARRAY_PROPERTY_PREFIX));
                    }
                    if (elements != null) {
                        for (final Element element : elements) {
                            if (element == null) {
                                strList.add(null);
                            } else {
                                final List<Object> argList = ParameterUtil
                                        .getValue(params, ARGS_QUERY_TYPE,
                                                Collections.emptyList());
                                try {
                                    final BeanDesc elementDesc = BeanDescFactory
                                            .getBeanDesc(element.getClass());
                                    final Method queryMethod = elementDesc
                                            .getMethod(queryType);
                                    strList.add(trimSpaces(
                                            (String) MethodUtil.invoke(
                                                    queryMethod,
                                                    element,
                                                    argList.toArray(new Object[argList
                                                            .size()])),
                                            isTrimSpaces));
                                } catch (final Exception e) {
                                    logger.warn("Could not invoke " + queryType
                                            + " on " + element, e);
                                    strList.add(null);
                                }
                            }
                        }
                        break;
                    }
                }
            }
            addPropertyData(dataMap, propName,
                    isArray ? strList : StringUtils.join(strList, " "));
        }

        storeIndex(responseData, dataMap);
    }

    protected Element[] getElements(final Element[] elements,
            final List<String> queries, final boolean isArrayProperty) {
        Element[] targets = elements;
        for (final String query : queries) {
            final List<Element> elementList = new ArrayList<Element>();
            for (final Element element : targets) {
                final Element[] childElements = getElements(
                        new Element[] { element }, query);
                if (childElements.length == 0 && isArrayProperty) {
                    elementList.add(null);
                } else {
                    for (final Element childElement : childElements) {
                        elementList.add(childElement);
                    }
                }
            }
            targets = elementList.toArray(new Element[elementList.size()]);
        }
        return targets;
    }

    protected Element[] getElements(final Element[] elements, final String query) {
        Element[] targets = elements;
        final Pattern pattern = Pattern
                .compile(":eq\\(([0-9]+)\\)|:lt\\(([0-9]+)\\)|:gt\\(([0-9]+)\\)");
        final Matcher matcher = pattern.matcher(query);
        final StringBuffer buf = new StringBuffer();
        while (matcher.find()) {
            final String value = matcher.group();
            matcher.appendReplacement(buf, "");
            if (buf.charAt(buf.length() - 1) != ' ') {
                try {
                    final int index = Integer.parseInt(matcher.group(1));
                    final List<Element> elementList = new ArrayList<Element>();
                    final String childQuery = buf.toString();
                    for (final Element element : targets) {
                        final Elements childElements = element
                                .select(childQuery);
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
                } catch (final NumberFormatException e) {
                    logger.warn("Invalid number: " + query, e);
                    buf.append(value);
                }
            } else {
                buf.append(value);
            }
        }
        matcher.appendTail(buf);
        final String lastQuery = buf.toString();
        if (StringUtil.isNotBlank(lastQuery)) {
            final List<Element> elementList = new ArrayList<Element>();
            for (final Element element : targets) {
                final Elements childElements = element.select(lastQuery);
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
                map = new LinkedHashMap<String, Object>();
                currentDataMap.put(currentKey, map);
            }
            currentDataMap = map;
        }
        currentDataMap.put(keys[keys.length - 1], value);
    }

    protected void storeIndex(final ResponseData responseData,
            final Map<String, Object> dataMap) {
        final String sessionId = responseData.getSessionId();
        final String indexName = riverConfig.getIndexName(sessionId);
        final String typeName = riverConfig.getTypeName(sessionId);
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

        @SuppressWarnings("unchecked")
        final Map<String, Object> arrayDataMap = (Map<String, Object>) dataMap
                .remove(ARRAY_PROPERTY_PREFIX);
        if (arrayDataMap != null) {
            final Map<String, Object> flatArrayDataMap = new LinkedHashMap<String, Object>();
            convertFlatMap("", arrayDataMap, flatArrayDataMap);
            int maxSize = 0;
            for (final Map.Entry<String, Object> entry : flatArrayDataMap
                    .entrySet()) {
                final Object value = entry.getValue();
                if (value instanceof List) {
                    @SuppressWarnings("rawtypes")
                    final int size = ((List) value).size();
                    if (size > maxSize) {
                        maxSize = size;
                    }
                }
            }
            for (int i = 0; i < maxSize; i++) {
                final Map<String, Object> newDataMap = new LinkedHashMap<String, Object>();
                newDataMap.put(POSITION_FIELD, i);
                deepCopy(dataMap, newDataMap);
                for (final Map.Entry<String, Object> entry : flatArrayDataMap
                        .entrySet()) {
                    final Object value = entry.getValue();
                    if (value instanceof List) {
                        @SuppressWarnings("unchecked")
                        final List<Object> list = (List<Object>) value;
                        if (i < list.size()) {
                            addPropertyData(newDataMap, entry.getKey(),
                                    list.get(i));
                        }
                    } else if (i == 0) {
                        addPropertyData(newDataMap, entry.getKey(), value);
                    }
                }
                storeIndex(client, indexName, typeName, newDataMap);
            }
        } else {
            storeIndex(client, indexName, typeName, dataMap);
        }
    }

    protected void storeIndex(final Client client, final String indexName,
            final String typeName, final Map<String, Object> dataMap) {
        dataMap.put(TIMESTAMP_FIELD, new Date());

        if (logger.isDebugEnabled()) {
            logger.debug(indexName + "/" + typeName + " : dataMap" + dataMap);
        }

        try {
            client.prepareIndex(indexName, typeName).setRefresh(true)
                    .setSource(jsonBuilder().value(dataMap)).execute()
                    .actionGet();
        } catch (final Exception e) {
            logger.warn("Could not write a content into index.", e);
        }
    }

    protected void deepCopy(final Map<String, Object> oldMap,
            final Map<String, Object> newMap) {
        final Map<String, Object> flatMap = new LinkedHashMap<String, Object>();
        convertFlatMap("", oldMap, flatMap);
        for (final Map.Entry<String, Object> entry : flatMap.entrySet()) {
            addPropertyData(newMap, entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    protected void convertFlatMap(final String prefix,
            final Map<String, Object> oldMap, final Map<String, Object> newMap) {
        for (final Map.Entry<String, Object> entry : oldMap.entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof Map) {
                convertFlatMap(prefix + entry.getKey() + ".",
                        (Map<String, Object>) value, newMap);
            } else {
                newMap.put(prefix + entry.getKey(), value);
            }
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
