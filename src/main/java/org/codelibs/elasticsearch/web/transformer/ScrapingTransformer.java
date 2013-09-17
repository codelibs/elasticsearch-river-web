package org.codelibs.elasticsearch.web.transformer;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.StringUtils;
import org.apache.xpath.objects.XObject;
import org.codelibs.elasticsearch.web.util.IdUtil;
import org.codelibs.elasticsearch.web.util.ParameterUtil;
import org.cyberneko.html.parsers.DOMParser;
import org.elasticsearch.client.Client;
import org.seasar.framework.beans.util.Beans;
import org.seasar.robot.RobotCrawlAccessException;
import org.seasar.robot.entity.AccessResultData;
import org.seasar.robot.entity.ResponseData;
import org.seasar.robot.entity.ResultData;
import org.seasar.robot.transformer.impl.XpathTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ScrapingTransformer extends
        org.seasar.robot.transformer.impl.HtmlTransformer {

    private static final Logger logger = LoggerFactory
            .getLogger(XpathTransformer.class);

    protected Map<String, Map<Pattern, Map<String, Map<String, String>>>> sessionPatternParamMap = new HashMap<String, Map<Pattern, Map<String, Map<String, String>>>>();

    protected Client client;

    protected Map<String, String> indexNameMap = new ConcurrentHashMap<String, String>();

    protected ObjectMapper objectMapper;

    private String[] copiedResonseDataFields = new String[] { "url",
            "parentUrl", "httpStatusCode", "method", "charSet",
            "contentLength", "mimeType", "executionTime", "lastModified" };

    @Override
    protected void storeData(final ResponseData responseData,
            final ResultData resultData) {
        final Map<String, Map<String, String>> scrapingRuleMap = getPatternParamMap(responseData);
        if (scrapingRuleMap == null) {
            return;
        }

        final DOMParser parser = getDomParser();
        try {
            final InputSource is = new InputSource(
                    responseData.getResponseBody());
            if (responseData.getCharSet() != null) {
                is.setEncoding(responseData.getCharSet());
            }
            parser.parse(is);
        } catch (final Exception e) {
            throw new RobotCrawlAccessException("Could not parse "
                    + responseData.getUrl(), e);
        }
        final Document document = parser.getDocument();

        final Map<String, Object> dataMap = new HashMap<String, Object>();
        Beans.copy(responseData, dataMap).includes(copiedResonseDataFields)
                .excludesNull().excludesWhitespace().execute();
        for (final Map.Entry<String, Map<String, String>> entry : scrapingRuleMap
                .entrySet()) {
            final Map<String, String> params = entry.getValue();
            final String path = params.get("path");
            final Boolean writeAsXml = ParameterUtil.getValue(params,
                    "writeAsXml", Boolean.FALSE);
            final Boolean isArray = ParameterUtil.getValue(params, "isArray",
                    Boolean.FALSE);
            try {
                final XObject xObj = getXPathAPI().eval(document, path);
                final int type = xObj.getType();
                switch (type) {
                case XObject.CLASS_BOOLEAN:
                    final boolean b = xObj.bool();
                    addPropertyData(dataMap, entry.getKey(),
                            Boolean.toString(b));
                    break;
                case XObject.CLASS_NUMBER:
                    final double d = xObj.num();
                    addPropertyData(dataMap, entry.getKey(), Double.toString(d));
                    break;
                case XObject.CLASS_STRING:
                    final String str = xObj.str();
                    addPropertyData(dataMap, entry.getKey(), str.trim());
                    break;
                case XObject.CLASS_NODESET:
                    final NodeList nodeList = xObj.nodelist();
                    final List<String> strList = new ArrayList<String>();
                    for (int i = 0; i < nodeList.getLength(); i++) {
                        final Node node = nodeList.item(i);
                        String content;
                        if (writeAsXml.booleanValue()) {
                            content = toXmlString(node);
                        } else {
                            content = node.getTextContent();
                        }
                        strList.add(content);
                    }
                    if (isArray.booleanValue()) {
                        addPropertyData(dataMap, entry.getKey(), strList);
                    } else {
                        addPropertyData(dataMap, entry.getKey(),
                                StringUtils.join(strList, null));
                    }
                    break;
                case XObject.CLASS_RTREEFRAG:
                    final int rtf = xObj.rtf();
                    addPropertyData(dataMap, entry.getKey(),
                            Integer.toString(rtf));
                    break;
                case XObject.CLASS_NULL:
                case XObject.CLASS_UNKNOWN:
                case XObject.CLASS_UNRESOLVEDVARIABLE:
                default:
                    Object obj = xObj.object();
                    if (obj == null) {
                        obj = "";
                    }
                    addPropertyData(dataMap, entry.getKey(), obj.toString());
                    break;
                }
            } catch (final TransformerException e) {
                logger.warn("Could not parse a value of " + entry.getKey()
                        + ":" + entry.getValue());
            }
        }

        storeIndex(responseData, dataMap);
    }

    private void addPropertyData(final Map<String, Object> dataMap,
            final String key, final Object value) {
        Map<String, Object> currentDataMap = dataMap;
        final String[] keys = key.split("\\.");
        for (int i = 0; i < keys.length - 1; i++) {
            final String currentKey = keys[i];
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
        final String indexName = getIndexName(sessionId);
        try {
            final String content = objectMapper.writeValueAsString(dataMap);
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

    public void addScrapingRule(final String sessionId,
            final Pattern urlPattern,
            final Map<String, Map<String, String>> scrapingRuleMap) {
        final Map<Pattern, Map<String, Map<String, String>>> patternParamMap = getPatternParamMap(sessionId);
        patternParamMap.put(urlPattern, scrapingRuleMap);
    }

    private Map<Pattern, Map<String, Map<String, String>>> getPatternParamMap(
            final String sessionId) {
        Map<Pattern, Map<String, Map<String, String>>> patternParamMap = sessionPatternParamMap
                .get(sessionId);
        if (patternParamMap == null) {
            patternParamMap = new HashMap<Pattern, Map<String, Map<String, String>>>();
            sessionPatternParamMap.put(sessionId, patternParamMap);
        }
        return patternParamMap;
    }

    private Map<String, Map<String, String>> getPatternParamMap(
            final ResponseData responseData) {
        final Map<Pattern, Map<String, Map<String, String>>> patternParamMap = getPatternParamMap(responseData
                .getSessionId());
        for (final Map.Entry<Pattern, Map<String, Map<String, String>>> entry : patternParamMap
                .entrySet()) {
            if (entry.getKey().matcher(responseData.getUrl()).matches()) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String toXmlString(final Node node) {
        try {
            final TransformerFactory transformerFactory = TransformerFactory
                    .newInstance();
            final Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
                    "yes");

            final StringWriter sw = new StringWriter();
            final StreamResult result = new StreamResult(sw);
            final DOMSource source = new DOMSource(node);
            transformer.transform(source, result);
            return sw.toString();
        } catch (final Exception e) {
            logger.warn("Could not convert a node to XML.", e);
        }
        return "";
    }

    public Client getClient() {
        return client;
    }

    public void setClient(final Client client) {
        this.client = client;
    }

    public String getIndexName(final String sessionId) {
        return indexNameMap.get(sessionId);
    }

    public void addIndexName(final String sessionId, final String indexName) {
        indexNameMap.put(sessionId, indexName);
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void setObjectMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void cleanup(final String sessionId) {
        indexNameMap.remove(sessionId);
        sessionPatternParamMap.remove(sessionId);
    }
}
