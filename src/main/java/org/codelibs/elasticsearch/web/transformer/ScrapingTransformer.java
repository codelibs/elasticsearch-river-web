package org.codelibs.elasticsearch.web.transformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.transform.TransformerException;

import org.apache.xpath.objects.XObject;
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

    protected Map<Pattern, Map<String, String>> scrapingPatternMap = new HashMap<Pattern, Map<String, String>>();

    protected Client client;

    protected String indexName;

    protected ObjectMapper objectMapper;

    private String[] copiedResonseDataFields = new String[] { "url",
            "parentUrl", "httpStatusCode", "method", "charSet",
            "contentLength", "mimeType", "executionTime", "lastModified" };

    @Override
    protected void storeData(final ResponseData responseData,
            final ResultData resultData) {
        final Map<String, String> scrapingRuleMap = getScrapingRuleMap(responseData);
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
                .execute();
        for (final Map.Entry<String, String> entry : scrapingRuleMap.entrySet()) {
            final String path = entry.getValue();
            try {
                final XObject xObj = getXPathAPI().eval(document, path);
                final int type = xObj.getType();
                switch (type) {
                case XObject.CLASS_BOOLEAN:
                    final boolean b = xObj.bool();
                    dataMap.put(entry.getKey(), Boolean.toString(b));
                    break;
                case XObject.CLASS_NUMBER:
                    final double d = xObj.num();
                    dataMap.put(entry.getKey(), Double.toString(d));
                    break;
                case XObject.CLASS_STRING:
                    final String str = xObj.str();
                    dataMap.put(entry.getKey(), str.trim());
                    break;
                case XObject.CLASS_NODESET:
                    final NodeList nodeList = xObj.nodelist();
                    final List<String> strList = new ArrayList<String>();
                    for (int i = 0; i < nodeList.getLength(); i++) {
                        final Node node = nodeList.item(i);
                        strList.add(node.getTextContent());
                    }
                    dataMap.put(entry.getKey(), strList);
                    break;
                case XObject.CLASS_RTREEFRAG:
                    final int rtf = xObj.rtf();
                    dataMap.put(entry.getKey(), Integer.toString(rtf));
                    break;
                case XObject.CLASS_NULL:
                case XObject.CLASS_UNKNOWN:
                case XObject.CLASS_UNRESOLVEDVARIABLE:
                default:
                    Object obj = xObj.object();
                    if (obj == null) {
                        obj = "";
                    }
                    dataMap.put(entry.getKey(), obj.toString());
                    break;
                }
            } catch (final TransformerException e) {
                logger.warn("Could not parse a value of " + entry.getKey()
                        + ":" + entry.getValue());
            }
        }

        try {
            final String content = objectMapper.writeValueAsString(dataMap);
            client.prepareIndex(indexName, responseData.getSessionId())
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

    public void addScrapingRule(final Pattern urlPattern,
            final Map<String, String> scrapingRuleMap) {
        scrapingPatternMap.put(urlPattern, scrapingRuleMap);
    }

    private Map<String, String> getScrapingRuleMap(
            final ResponseData responseData) {
        for (final Map.Entry<Pattern, Map<String, String>> entry : scrapingPatternMap
                .entrySet()) {
            if (entry.getKey().matcher(responseData.getUrl()).matches()) {
                return entry.getValue();
            }
        }
        return null;
    }

    public Client getClient() {
        return client;
    }

    public void setClient(final Client client) {
        this.client = client;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(final String indexName) {
        this.indexName = indexName;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void setObjectMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
}
