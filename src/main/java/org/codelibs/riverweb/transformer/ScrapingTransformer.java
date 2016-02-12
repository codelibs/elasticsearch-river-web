package org.codelibs.riverweb.transformer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.codelibs.core.beans.BeanDesc;
import org.codelibs.core.beans.factory.BeanDescFactory;
import org.codelibs.core.beans.util.BeanUtil;
import org.codelibs.core.io.CopyUtil;
import org.codelibs.core.io.FileUtil;
import org.codelibs.core.lang.MethodUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.misc.Base64Util;
import org.codelibs.fess.crawler.Constants;
import org.codelibs.fess.crawler.builder.RequestDataBuilder;
import org.codelibs.fess.crawler.client.EsClient;
import org.codelibs.fess.crawler.entity.AccessResultData;
import org.codelibs.fess.crawler.entity.RequestData;
import org.codelibs.fess.crawler.entity.ResponseData;
import org.codelibs.fess.crawler.entity.ResultData;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.helper.EncodingHelper;
import org.codelibs.fess.crawler.transformer.impl.HtmlTransformer;
import org.codelibs.riverweb.WebRiverConstants;
import org.codelibs.riverweb.app.service.ScriptService;
import org.codelibs.riverweb.config.RiverConfig;
import org.codelibs.riverweb.config.RiverConfigManager;
import org.codelibs.riverweb.entity.ScrapingRule;
import org.codelibs.riverweb.util.SettingsUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.lastaflute.di.core.SingletonLaContainer;
import org.lastaflute.di.core.factory.SingletonLaContainerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScrapingTransformer extends HtmlTransformer {

    private static final long DEFAULT_MAX_ATTACHMENT_SIZE = 1000 * 1000; // 1M

    private static final String VALUE_QUERY_TYPE = "value";

    private static final String TYPE_QUERY_TYPE = "type";

    private static final String SCRIPT_QUERY_TYPE = "script";

    private static final String ARGS_QUERY_TYPE = "args";

    private static final String IS_ARRAY_PROP_NAME = "is_array";

    private static final String IS_DISTINCT_PROP_NAME = "is_distinct";

    private static final String IS_CHILD_URL_PROP_NAME = "is_child";

    private static final String TRIM_SPACES_PROP_NAME = "trim_spaces";

    private static final String TIMESTAMP_FIELD = "@timestamp";

    private static final String POSITION_FIELD = "position";

    private static final String ARRAY_PROPERTY_PREFIX = "[]";

    private static final Logger logger = LoggerFactory.getLogger(ScrapingTransformer.class);

    private static final String[] queryTypes = new String[] { "className", "data", "html", "id", "ownText", "tagName", "text", "val",
            "nodeName", "outerHtml", "attr", "baseUri", "absUrl" };

    public String[] copiedResonseDataFields = new String[] { "url", "parentUrl", "httpStatusCode", "method", "charSet", "contentLength",
            "mimeType", "executionTime", "lastModified" };

    private EsClient esClient;

    protected RiverConfigManager riverConfigManager;

    protected ThreadLocal<Set<String>> childUrlSetLocal = new ThreadLocal<Set<String>>();

    protected ThreadLocal<RiverConfig> riverConfigLocal = new ThreadLocal<>();


    @PostConstruct
    public void init() {
        esClient = SingletonLaContainer.getComponent(EsClient.class);
        riverConfigManager = SingletonLaContainer.getComponent(RiverConfigManager.class);
    }

    @Override
    public ResultData transform(final ResponseData responseData) {
        final RiverConfig riverConfig = riverConfigManager.get(responseData.getSessionId());

        try {
            riverConfigLocal.set(riverConfig);
            return super.transform(responseData);
        } finally {
            riverConfigLocal.remove();
            childUrlSetLocal.remove();
        }
    }

    @Override
    protected void updateCharset(final ResponseData responseData) {
        int preloadSize = preloadSizeForCharset;
        final ScrapingRule scrapingRule = riverConfigLocal.get().getScrapingRule(responseData);
        if (scrapingRule != null) {
            final Integer s = scrapingRule.getSetting("preloadSizeForCharset", Integer.valueOf(0));
            if (s.intValue() > 0) {
                preloadSize = s.intValue();
            }
        }
        final String encoding = loadCharset(responseData.getResponseBody(), preloadSize);
        if (encoding == null) {
            if (defaultEncoding == null) {
                responseData.setCharSet(Constants.UTF_8);
            } else if (responseData.getCharSet() == null) {
                responseData.setCharSet(defaultEncoding);
            }
        } else {
            responseData.setCharSet(encoding.trim());
        }

        if (!isSupportedCharset(responseData.getCharSet())) {
            responseData.setCharSet(Constants.UTF_8);
        }
    }

    protected String loadCharset(final InputStream inputStream, final int preloadSize) {
        BufferedInputStream bis = null;
        String encoding = null;
        try {
            bis = new BufferedInputStream(inputStream);
            final byte[] buffer = new byte[preloadSize];
            final int size = bis.read(buffer);
            if (size != -1) {
                final String content = new String(buffer, 0, size);
                encoding = parseCharset(content);
            }
        } catch (final IOException e) {
            throw new CrawlingAccessException("Could not load a content.", e);
        }

        try {
            final EncodingHelper encodingHelper = SingletonLaContainer.getComponent(EncodingHelper.class);
            encoding = encodingHelper.normalize(encoding);
        } catch (final Exception e) {
            // NOP
        }

        return encoding;
    }

    @Override
    protected void storeData(final ResponseData responseData, final ResultData resultData) {
        File file = null;
        try {
            final ScrapingRule scrapingRule = riverConfigLocal.get().getScrapingRule(responseData);
            if (scrapingRule == null) {
                logger.info("Skip Scraping: " + responseData.getUrl());
                return;
            }

            file = File.createTempFile("river-web-", ".tmp");
            CopyUtil.copy(responseData.getResponseBody(), file);
            processData(scrapingRule, file, responseData, resultData);
        } catch (final IOException e) {
            throw new CrawlingAccessException("Failed to create a temp file.", e);
        } finally {
            if (file != null && !file.delete()) {
                logger.warn("Failed to delete " + file.getAbsolutePath());
            }
        }
    }

    protected void processData(final ScrapingRule scrapingRule, final File file, final ResponseData responseData,
            final ResultData resultData) {
        final Map<String, Map<String, Object>> scrapingRuleMap = scrapingRule.getRuleMap();

        org.jsoup.nodes.Document document = null;
        String charsetName = responseData.getCharSet();
        if (charsetName == null) {
            charsetName = Constants.UTF_8;
        }

        final Boolean isHtmlParsed = scrapingRule.getSetting("html", Boolean.TRUE);
        if (isHtmlParsed.booleanValue()) {
            try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
                document = Jsoup.parse(is, charsetName, responseData.getUrl());
            } catch (final IOException e) {
                throw new CrawlingAccessException("Could not parse " + responseData.getUrl(), e);
            }
        }

        final Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
        BeanUtil.copyBeanToMap(responseData, dataMap, op -> {
            op.include(copiedResonseDataFields).excludeNull().excludeWhitespace();
        });
        if (logger.isDebugEnabled()) {
            logger.debug("ruleMap: " + scrapingRuleMap);
            logger.debug("dataMap: " + dataMap);
        }
        for (final Map.Entry<String, Map<String, Object>> entry : scrapingRuleMap.entrySet()) {
            final String propName = entry.getKey();
            final Map<String, Object> params = entry.getValue();
            final boolean isTrimSpaces = SettingsUtils.get(params, TRIM_SPACES_PROP_NAME, Boolean.FALSE).booleanValue();
            boolean isArray = SettingsUtils.get(params, IS_ARRAY_PROP_NAME, Boolean.FALSE).booleanValue();
            boolean isChildUrl = SettingsUtils.get(params, IS_CHILD_URL_PROP_NAME, Boolean.FALSE).booleanValue();
            boolean isDistinct = SettingsUtils.get(params, IS_DISTINCT_PROP_NAME, Boolean.FALSE).booleanValue();

            final List<String> strList = new ArrayList<String>();

            final Object value = SettingsUtils.get(params, VALUE_QUERY_TYPE, null);
            final String type = SettingsUtils.get(params, TYPE_QUERY_TYPE, null);
            if (value != null) {
                if (value instanceof String) {
                    strList.add(trimSpaces(value.toString(), isTrimSpaces));
                } else if (value instanceof List) {
                    @SuppressWarnings("unchecked")
                    final List<Object> list = (List<Object>) value;
                    for (final Object obj : list) {
                        strList.add(trimSpaces(obj.toString(), isTrimSpaces));
                    }
                }
            } else if ("data".equals(type) || "attachment".equals(type)) {
                final long maxFileSize = SettingsUtils.get(params, "maxFileSize", DEFAULT_MAX_ATTACHMENT_SIZE);
                final long fileSize = file.length();
                if (fileSize <= maxFileSize) {
                    strList.add(Base64Util.encode(FileUtil.readBytes(file)));
                    isArray = false;
                    isChildUrl = false;
                    isDistinct = false;
                } else {
                    logger.info("The max file size(" + fileSize + "/" + maxFileSize + " is exceeded: " + responseData.getUrl());
                }
            } else if ("source".equals(type)) {
                try {
                    strList.add(trimSpaces(FileUtil.readText(file, charsetName), isTrimSpaces));
                } catch (Exception e) {
                    logger.warn("Failed to read type:source from " + responseData.getUrl(), e);
                }
            } else if (document != null) {
                processCssQuery(document, propName, params, isTrimSpaces, strList);
            }

            Object propertyValue;
            final ScriptInfo scriptInfo = getScriptValue(params);
            if (isDistinct) {
                final Set<String> strSet = new HashSet<>();
                final List<String> distinctList = strList.stream().filter(s -> strSet.add(s) && (!isTrimSpaces || StringUtil.isNotBlank(s)))
                        .collect(Collectors.toList());
                strList.clear();
                strList.addAll(distinctList);
            }
            if (scriptInfo == null) {
                propertyValue = isArray ? strList : String.join(" ", strList);
            } else {
                final Map<String, Object> vars = new HashMap<String, Object>();
                vars.put("container", SingletonLaContainerFactory.getContainer());
                vars.put("client", esClient);
                vars.put("data", responseData);
                vars.put("result", resultData);
                vars.put("property", propName);
                vars.put("parameters", params);
                vars.put("array", isArray);
                vars.put("list", strList);
                if (isArray) {
                    final List<Object> list = new ArrayList<Object>();
                    for (int i = 0; i < strList.size(); i++) {
                        final Map<String, Object> localVars = new HashMap<String, Object>(vars);
                        localVars.put("index", i);
                        localVars.put("value", String.join(" ", strList));
                        list.add(executeScript(scriptInfo.getLang(), scriptInfo.getScript(), scriptInfo.getScriptType(), localVars));
                    }
                    propertyValue = list;
                } else {
                    vars.put("value", String.join(" ", strList));
                    propertyValue = executeScript(scriptInfo.getLang(), scriptInfo.getScript(), scriptInfo.getScriptType(), vars);
                }
            }
            addPropertyData(dataMap, propName, propertyValue);
            if (isChildUrl) {
                Set<String> childUrlSet = childUrlSetLocal.get();
                if (childUrlSet == null) {
                    childUrlSet = new HashSet<String>();
                    childUrlSetLocal.set(childUrlSet);
                }
                if (propertyValue instanceof String) {
                    final String str = (String) propertyValue;
                    if (StringUtil.isNotBlank(str)) {
                        childUrlSet.add(str);
                    }
                } else if (propertyValue instanceof List) {
                    @SuppressWarnings("unchecked")
                    final List<Object> list = (List<Object>) propertyValue;
                    for (final Object obj : list) {
                        final String str = obj.toString();
                        if (StringUtil.isNotBlank(str)) {
                            childUrlSet.add(str);
                        }
                    }
                }
            }
        }

        storeIndex(responseData, dataMap);
    }

    private Object executeScript(final String lang, final String script, final String scriptTypeValue, final Map<String, Object> vars) {
        ScriptType scriptType;
        if (ScriptType.FILE.toString().equalsIgnoreCase(scriptTypeValue)) {
            scriptType = ScriptType.FILE;
        } else if (ScriptType.INDEXED.toString().equalsIgnoreCase(scriptTypeValue)) {
            scriptType = ScriptType.INDEXED;
        } else {
            scriptType = ScriptType.INLINE;
        }
        vars.put("logger", logger);
        final ScriptService scriptService = SingletonLaContainer.getComponent(ScriptService.class);
        return scriptService.execute(lang, script, scriptType, vars);
    }

    protected ScriptInfo getScriptValue(final Map<String, Object> params) {
        final Object value = SettingsUtils.get(params, SCRIPT_QUERY_TYPE, null);
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return new ScriptInfo(value.toString());
        } else if (value instanceof List) {
            @SuppressWarnings("unchecked")
            final List<CharSequence> list = (List<CharSequence>) value;
            return new ScriptInfo(String.join("", list));
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> scriptMap = (Map<String, Object>) value;
            final String script = SettingsUtils.get(scriptMap, SCRIPT_QUERY_TYPE);
            if (script == null) {
                return null;
            }
            return new ScriptInfo(script, SettingsUtils.get(scriptMap, "lang", WebRiverConstants.DEFAULT_SCRIPT_LANG),
                    SettingsUtils.get(scriptMap, "script_type", "inline"));
        }
        return null;
    }

    private static class ScriptInfo {
        private final String script;

        private final String lang;

        private final String scriptType;

        ScriptInfo(final String script) {
            this(script, WebRiverConstants.DEFAULT_SCRIPT_LANG, "inline");
        }

        ScriptInfo(final String script, final String lang, final String scriptType) {
            this.script = script;
            this.lang = lang;
            this.scriptType = scriptType;
        }

        public String getScript() {
            return script;
        }

        public String getLang() {
            return lang;
        }

        public String getScriptType() {
            return scriptType;
        }
    }

    protected void processCssQuery(final org.jsoup.nodes.Document document, final String propName, final Map<String, Object> params,
            final boolean isTrimSpaces, final List<String> strList) {
        for (final String queryType : queryTypes) {
            final Object queryObj = SettingsUtils.get(params, queryType, null);
            Element[] elements = null;
            if (queryObj instanceof String) {
                elements = getElements(new Element[] { document }, queryObj.toString());
            } else if (queryObj instanceof List) {
                @SuppressWarnings("unchecked")
                final List<String> queryList = (List<String>) queryObj;
                elements = getElements(new Element[] { document }, queryList, propName.startsWith(ARRAY_PROPERTY_PREFIX));
            }
            if (elements != null) {
                for (final Element element : elements) {
                    if (element == null) {
                        strList.add(null);
                    } else {
                        final List<Object> argList = SettingsUtils.get(params, ARGS_QUERY_TYPE, Collections.emptyList());
                        try {
                            final Method queryMethod = getQueryMethod(element, queryType, argList);
                            strList.add(trimSpaces(
                                    (String) MethodUtil.invoke(queryMethod, element, argList.toArray(new Object[argList.size()])),
                                    isTrimSpaces));
                        } catch (final Exception e) {
                            logger.warn("Could not invoke " + queryType + " on " + element, e);
                            strList.add(null);
                        }
                    }
                }
                break;
            }
        }
    }

    protected Method getQueryMethod(final Element element, final String queryType, final List<Object> argList) {
        final BeanDesc elementDesc = BeanDescFactory.getBeanDesc(element.getClass());
        if (argList == null || argList.isEmpty()) {
            return elementDesc.getMethodDesc(queryType).getMethod();
        } else {
            final Class<?>[] paramTypes = new Class[argList.size()];
            for (int i = 0; i < paramTypes.length; i++) {
                paramTypes[i] = String.class;
            }
            return elementDesc.getMethodDesc(queryType, paramTypes).getMethod();
        }
    }

    protected Element[] getElements(final Element[] elements, final List<String> queries, final boolean isArrayProperty) {
        Element[] targets = elements;
        for (final String query : queries) {
            final List<Element> elementList = new ArrayList<Element>();
            for (final Element element : targets) {
                if (element == null) {
                    elementList.add(null);
                } else {
                    final Element[] childElements = getElements(new Element[] { element }, query);
                    if (childElements.length == 0 && isArrayProperty) {
                        elementList.add(null);
                    } else {
                        for (final Element childElement : childElements) {
                            elementList.add(childElement);
                        }
                    }
                }
            }
            targets = elementList.toArray(new Element[elementList.size()]);
        }
        return targets;
    }

    protected Element[] getElements(final Element[] elements, final String query) {
        Element[] targets = elements;
        final Pattern pattern = Pattern.compile(":eq\\(([0-9]+)\\)|:lt\\(([0-9]+)\\)|:gt\\(([0-9]+)\\)");
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
                        final Elements childElements = element.select(childQuery);
                        if (value.startsWith(":eq")) {
                            if (index < childElements.size()) {
                                elementList.add(childElements.get(index));
                            }
                        } else if (value.startsWith(":lt")) {
                            for (int i = 0; i < childElements.size() && i < index; i++) {
                                elementList.add(childElements.get(i));
                            }
                        } else if (value.startsWith(":gt")) {
                            for (int i = index + 1; i < childElements.size(); i++) {
                                elementList.add(childElements.get(i));
                            }
                        }
                    }
                    targets = elementList.toArray(new Element[elementList.size()]);
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
                if (element == null) {
                    elementList.add(null);
                } else {
                    final Elements childElements = element.select(lastQuery);
                    for (int i = 0; i < childElements.size(); i++) {
                        elementList.add(childElements.get(i));
                    }
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

    protected void addPropertyData(final Map<String, Object> dataMap, final String key, final Object value) {
        Map<String, Object> currentDataMap = dataMap;
        final String[] keys = key.split("\\.");
        for (int i = 0; i < keys.length - 1; i++) {
            final String currentKey = keys[i];
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) currentDataMap.get(currentKey);
            if (map == null) {
                map = new LinkedHashMap<String, Object>();
                currentDataMap.put(currentKey, map);
            }
            currentDataMap = map;
        }
        currentDataMap.put(keys[keys.length - 1], value);
    }

    protected void storeIndex(final ResponseData responseData, final Map<String, Object> dataMap) {
        final String sessionId = responseData.getSessionId();
        final RiverConfig riverConfig = riverConfigLocal.get();
        final String indexName = riverConfig.getIndex();
        final String typeName = riverConfig.getType();
        final boolean overwrite = riverConfig.isOverwrite();

        if (logger.isDebugEnabled()) {
            logger.debug("Index: " + indexName + ", sessionId: " + sessionId + ", Data: " + dataMap);
        }

        if (overwrite) {
            final int count = esClient.deleteByQuery(indexName, typeName, QueryBuilders.termQuery("url", responseData.getUrl()));
            if (count > 0) {
                esClient.admin().indices().prepareRefresh(indexName).execute().actionGet();
            }
        }

        @SuppressWarnings("unchecked")
        final Map<String, Object> arrayDataMap = (Map<String, Object>) dataMap.remove(ARRAY_PROPERTY_PREFIX);
        if (arrayDataMap != null) {
            final Map<String, Object> flatArrayDataMap = new LinkedHashMap<String, Object>();
            convertFlatMap("", arrayDataMap, flatArrayDataMap);
            int maxSize = 0;
            for (final Map.Entry<String, Object> entry : flatArrayDataMap.entrySet()) {
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
                for (final Map.Entry<String, Object> entry : flatArrayDataMap.entrySet()) {
                    final Object value = entry.getValue();
                    if (value instanceof List) {
                        @SuppressWarnings("unchecked")
                        final List<Object> list = (List<Object>) value;
                        if (i < list.size()) {
                            addPropertyData(newDataMap, entry.getKey(), list.get(i));
                        }
                    } else if (i == 0) {
                        addPropertyData(newDataMap, entry.getKey(), value);
                    }
                }
                storeIndex(indexName, typeName, newDataMap);
            }
        } else {
            storeIndex(indexName, typeName, dataMap);
        }
    }

    protected void storeIndex(final String indexName, final String typeName, final Map<String, Object> dataMap) {
        dataMap.put(TIMESTAMP_FIELD, new Date());

        if (logger.isDebugEnabled()) {
            logger.debug(indexName + "/" + typeName + " : dataMap" + dataMap);
        }

        try {
            esClient.prepareIndex(indexName, typeName).setRefresh(true).setSource(jsonBuilder().value(dataMap)).execute().actionGet();
        } catch (final Exception e) {
            logger.warn("Could not write a content into index.", e);
        }
    }

    protected void deepCopy(final Map<String, Object> oldMap, final Map<String, Object> newMap) {
        final Map<String, Object> flatMap = new LinkedHashMap<String, Object>();
        convertFlatMap("", oldMap, flatMap);
        for (final Map.Entry<String, Object> entry : flatMap.entrySet()) {
            addPropertyData(newMap, entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    protected void convertFlatMap(final String prefix, final Map<String, Object> oldMap, final Map<String, Object> newMap) {
        for (final Map.Entry<String, Object> entry : oldMap.entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof Map) {
                convertFlatMap(prefix + entry.getKey() + ".", (Map<String, Object>) value, newMap);
            } else {
                newMap.put(prefix + entry.getKey(), value);
            }
        }
    }

    @Override
    protected void storeChildUrls(final ResponseData responseData, final ResultData resultData) {
        final Set<String> childLinkSet = childUrlSetLocal.get();
        if (childLinkSet != null) {
            List<RequestData> requestDataList = convertChildUrlList(childLinkSet.stream().filter(u -> StringUtil.isNotBlank(u))
                    .map(u -> RequestDataBuilder.newRequestData().get().url(u).build()).collect(Collectors.toList()));
            resultData.addAllUrl(requestDataList);

            final RequestData requestData = responseData.getRequestData();
            resultData.removeUrl(requestData);
            resultData.removeUrl(getDuplicateUrl(requestData));
        } else {
            super.storeChildUrls(responseData, resultData);
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
