package org.codelibs.elasticsearch.web.transformer;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.StringUtils;
import org.apache.xpath.objects.XObject;
import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.codelibs.elasticsearch.web.util.IdUtil;
import org.codelibs.elasticsearch.web.util.ParameterUtil;
import org.cyberneko.html.parsers.DOMParser;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.seasar.framework.beans.util.Beans;
import org.seasar.framework.container.SingletonS2Container;
import org.seasar.framework.container.annotation.tiger.InitMethod;
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

public class ScrapingTransformer extends
        org.seasar.robot.transformer.impl.HtmlTransformer {

    private static final Logger logger = LoggerFactory
            .getLogger(XpathTransformer.class);

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
        for (final Map.Entry<String, Map<String, Object>> entry : scrapingRuleMap
                .entrySet()) {
            final Map<String, Object> params = entry.getValue();
            final String path = (String) params.get("path");
            final Boolean writeAsXml = ParameterUtil.getValue(params,
                    "writeAsXml", Boolean.FALSE);
            final boolean isArray = ParameterUtil.getValue(params, "isArray",
                    Boolean.FALSE).booleanValue();
            final boolean isTrimSpaces = ParameterUtil.getValue(params,
                    "trimSpaces", Boolean.FALSE).booleanValue();
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
                    addPropertyData(dataMap, entry.getKey(),
                            trimSpaces(str, isTrimSpaces));
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
                        strList.add(trimSpaces(content, isTrimSpaces));
                    }
                    if (isArray) {
                        addPropertyData(dataMap, entry.getKey(), strList);
                    } else {
                        addPropertyData(dataMap, entry.getKey(),
                                StringUtils.join(strList, " "));
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
                    addPropertyData(dataMap, entry.getKey(),
                            trimSpaces(obj.toString(), isTrimSpaces));
                    break;
                }
            } catch (final TransformerException e) {
                logger.warn("Could not parse a value of " + entry.getKey()
                        + ":" + entry.getValue());
            }
        }

        storeIndex(responseData, dataMap);
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

}
