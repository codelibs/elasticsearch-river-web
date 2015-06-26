package org.codelibs.elasticsearch.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntConsumer;

import javax.annotation.Resource;

import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.auth.NTLMScheme;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.misc.DynamicProperties;
import org.codelibs.elasticsearch.web.app.service.ScriptService;
import org.codelibs.elasticsearch.web.client.EsClient;
import org.codelibs.elasticsearch.web.entity.RiverConfig;
import org.codelibs.elasticsearch.web.interval.WebRiverIntervalController;
import org.codelibs.elasticsearch.web.util.SettingsUtils;
import org.codelibs.robot.S2Robot;
import org.codelibs.robot.S2RobotContext;
import org.codelibs.robot.client.http.Authentication;
import org.codelibs.robot.client.http.HcHttpClient;
import org.codelibs.robot.client.http.RequestHeader;
import org.codelibs.robot.client.http.impl.AuthenticationImpl;
import org.codelibs.robot.client.http.ntlm.JcifsEngine;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.lastaflute.di.core.SingletonLaContainer;
import org.lastaflute.di.core.factory.SingletonLaContainerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiverWeb {
    private static final Logger logger = LoggerFactory.getLogger(RiverWeb.class);

    private static final String NTLM_SCHEME = "NTLM";

    private static final String DIGEST_SCHEME = "DIGEST";

    private static final String BASIC_SCHEME = "BASIC";

    @Option(name = "--config-id", required = true)
    protected String configId;

    @Option(name = "--session-id")
    protected String sessionId;

    @Option(name = "--cleanup")
    protected boolean cleanup;

    @Option(name = "--es-host")
    protected String esHost;

    @Option(name = "--es-port")
    protected String esPort;

    @Option(name = "--cluster-name")
    protected String clusterName;

    @Resource
    protected Client esClient;

    @Resource
    protected DynamicProperties config;

    @Resource
    protected ScriptService scriptService;

    @Resource
    protected S2Robot s2Robot;

    @Resource
    protected RiverConfig riverConfig;

    @Resource
    protected String defaultUserAgent;

    protected static IntConsumer exitMethod = System::exit;

    public static void main(final String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SingletonLaContainerFactory.destroy();
            }
        });

        SingletonLaContainerFactory.init();
        final RiverWeb riverWeb = SingletonLaContainer.getComponent(RiverWeb.class);

        final CmdLineParser parser = new CmdLineParser(riverWeb, ParserProperties.defaults().withUsageWidth(80));
        try {
            parser.parseArgument(args);
        } catch (final Exception e) {
            parser.printUsage(System.out);
            exitMethod.accept(1);
            return;
        }

        exitMethod.accept(riverWeb.execute());
        SingletonLaContainerFactory.destroy();
    }

    private void print(final String format, final Object... args) {
        System.out.println(String.format(format, args));
    }

    private int execute() {
        // update esClient
        final String elasticsearchClusterName =
                clusterName == null ? config.getProperty("elasticsearch.cluster.name", "elasticsearch") : clusterName;
        final String elasticsearchHost = esHost == null ? config.getProperty("elasticsearch.host", "localhost") : esHost;
        final int elasticsearchPort = Integer.parseInt(esPort == null ? config.getProperty("elasticsearch.port", "9300") : esPort);
        ((EsClient) esClient).connect(elasticsearchClusterName, elasticsearchHost, elasticsearchPort);

        // Load config data
        final String configIndex = config.getProperty("config.index");
        final String configType = config.getProperty("config.type");
        final GetResponse response = esClient.prepareGet(configIndex, configType, configId).execute().actionGet();
        if (!response.isExists()) {
            print("Config ID {} is not found in {}/{}.", configId, configIndex, configType);
            return 1;
        }

        final Map<String, Object> crawlSettings = response.getSource();

        if (StringUtil.isBlank(sessionId)) {
            sessionId = UUID.randomUUID().toString();
        }

        final Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("configId", configId);
        vars.put("client", esClient);
        vars.put("sessionId", sessionId);

        try {
            // invoke execute event script
            executeScript(crawlSettings, vars, "execute");

            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> targetList = (List<Map<String, Object>>) crawlSettings.get("target");
            if (targetList == null || targetList.isEmpty()) {
                print("No targets for crawling.");
                return 1;
            }

            s2Robot.setSessionId(sessionId);

            // HttpClient Parameters
            final Map<String, Object> paramMap = new HashMap<String, Object>();
            s2Robot.getClientFactory().setInitParameterMap(paramMap);

            // user agent
            final String userAgent = SettingsUtils.get(crawlSettings, "userAgent", defaultUserAgent);
            if (StringUtil.isNotBlank(userAgent)) {
                paramMap.put(HcHttpClient.USER_AGENT_PROPERTY, userAgent);
            }

            // robots.txt parser
            final Boolean robotsTxtEnabled = SettingsUtils.get(crawlSettings, "robotsTxt", Boolean.TRUE);
            paramMap.put(HcHttpClient.ROBOTS_TXT_ENABLED_PROPERTY, robotsTxtEnabled);

            // proxy
            final Map<String, Object> proxyMap = SettingsUtils.get(crawlSettings, "proxy", null);
            if (proxyMap != null) {
                final Object host = proxyMap.get("host");
                if (host != null) {
                    paramMap.put(HcHttpClient.PROXY_HOST_PROPERTY, host);
                    final Object portObj = proxyMap.get("port");
                    if (portObj instanceof Integer) {
                        paramMap.put(HcHttpClient.PROXY_PORT_PROPERTY, portObj);
                    } else {
                        paramMap.put(HcHttpClient.PROXY_PORT_PROPERTY, Integer.valueOf(8080));
                    }
                }
            }

            // authentications
            // "authentications":[{"scope":{"scheme":"","host":"","port":0,"realm":""},
            //   "credentials":{"username":"","password":""}},{...}]
            final List<Map<String, Object>> authList = SettingsUtils.get(crawlSettings, "authentications", null);
            if (authList != null && !authList.isEmpty()) {
                final List<Authentication> basicAuthList = new ArrayList<Authentication>();
                for (final Map<String, Object> authObj : authList) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> scopeMap = (Map<String, Object>) authObj.get("scope");
                    String scheme = SettingsUtils.get(scopeMap, "scheme", StringUtil.EMPTY).toUpperCase(Locale.ENGLISH);
                    if (StringUtil.isBlank(scheme)) {
                        logger.warn("Invalid authentication: " + authObj);
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> credentialMap = (Map<String, Object>) authObj.get("credentials");
                    final String username = SettingsUtils.get(credentialMap, "username", null);
                    if (StringUtil.isBlank(username)) {
                        logger.warn("Invalid authentication: " + authObj);
                        continue;
                    }
                    final String host = SettingsUtils.get(authObj, "host", AuthScope.ANY_HOST);
                    final int port = SettingsUtils.get(authObj, "port", AuthScope.ANY_PORT);
                    final String realm = SettingsUtils.get(authObj, "realm", AuthScope.ANY_REALM);
                    final String password = SettingsUtils.get(credentialMap, "password", null);

                    AuthScheme authScheme = null;
                    Credentials credentials = null;
                    if (BASIC_SCHEME.equalsIgnoreCase(scheme)) {
                        authScheme = new BasicScheme();
                        credentials = new UsernamePasswordCredentials(username, password);
                    } else if (DIGEST_SCHEME.equals(scheme)) {
                        authScheme = new DigestScheme();
                        credentials = new UsernamePasswordCredentials(username, password);
                    } else if (NTLM_SCHEME.equals(scheme)) {
                        authScheme = new NTLMScheme(new JcifsEngine());
                        scheme = AuthScope.ANY_SCHEME;
                        final String workstation = SettingsUtils.get(credentialMap, "workstation", null);
                        final String domain = SettingsUtils.get(credentialMap, "domain", null);
                        credentials =
                                new NTCredentials(username, password, workstation == null ? StringUtil.EMPTY : workstation,
                                        domain == null ? StringUtil.EMPTY : domain);
                    }

                    final AuthenticationImpl auth =
                            new AuthenticationImpl(new AuthScope(host, port, realm, scheme), credentials, authScheme);
                    basicAuthList.add(auth);
                }
                paramMap.put(HcHttpClient.BASIC_AUTHENTICATIONS_PROPERTY, basicAuthList.toArray(new Authentication[basicAuthList.size()]));
            }

            // request header
            // "headers":[{"name":"","value":""},{}]
            final List<Map<String, Object>> headerList = SettingsUtils.get(crawlSettings, "headers", null);
            if (headerList != null && !headerList.isEmpty()) {
                final List<RequestHeader> requestHeaderList = new ArrayList<RequestHeader>();
                for (final Map<String, Object> headerObj : headerList) {
                    final String name = SettingsUtils.get(headerObj, "name", null);
                    final String value = SettingsUtils.get(headerObj, "value", null);
                    if (name != null && value != null) {
                        requestHeaderList.add(new RequestHeader(name, value));
                    }
                }
                paramMap.put(HcHttpClient.REQUERT_HEADERS_PROPERTY, requestHeaderList.toArray(new RequestHeader[requestHeaderList.size()]));
            }

            // url
            @SuppressWarnings("unchecked")
            final List<String> urlList = (List<String>) crawlSettings.get("url");
            if (urlList == null || urlList.isEmpty()) {
                print("No url for crawling.");
                return 1;
            }
            for (final String url : urlList) {
                s2Robot.addUrl(url);
            }
            // include regex
            @SuppressWarnings("unchecked")
            final List<String> includeFilterList = (List<String>) crawlSettings.get("includeFilter");
            if (includeFilterList != null) {
                for (final String regex : includeFilterList) {
                    s2Robot.addIncludeFilter(regex);
                }
            }
            // exclude regex
            @SuppressWarnings("unchecked")
            final List<String> excludeFilterList = (List<String>) crawlSettings.get("excludeFilter");
            if (excludeFilterList != null) {
                for (final String regex : excludeFilterList) {
                    s2Robot.addExcludeFilter(regex);
                }
            }

            final S2RobotContext robotContext = s2Robot.getRobotContext();

            // max depth
            final int maxDepth = SettingsUtils.get(crawlSettings, "maxDepth", -1);

            robotContext.setMaxDepth(maxDepth);
            // max access count
            final int maxAccessCount = SettingsUtils.get(crawlSettings, "maxAccessCount", 100);
            robotContext.setMaxAccessCount(maxAccessCount);
            // num of thread
            final int numOfThread = SettingsUtils.get(crawlSettings, "numOfThread", 5);
            robotContext.setNumOfThread(numOfThread);
            // interval
            final long interval = SettingsUtils.get(crawlSettings, "interval", 1000L);
            final WebRiverIntervalController intervalController = (WebRiverIntervalController) s2Robot.getIntervalController();
            intervalController.setDelayMillisForWaitingNewUrl(interval);

            // river params
            riverConfig.setIndex(SettingsUtils.get(crawlSettings, "index", "web"));
            riverConfig.setType(SettingsUtils.get(crawlSettings, "type", configId));
            riverConfig.setOverwrite(SettingsUtils.get(crawlSettings, "overwrite", Boolean.FALSE));
            riverConfig.setIncremental(SettingsUtils.get(crawlSettings, "incremental", Boolean.FALSE));

            // crawl config
            for (final Map<String, Object> targetMap : targetList) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> patternMap = (Map<String, Object>) targetMap.get("pattern");
                @SuppressWarnings("unchecked")
                final Map<String, Map<String, Object>> propMap = (Map<String, Map<String, Object>>) targetMap.get("properties");
                if (patternMap != null && propMap != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("patternMap: " + patternMap);
                        logger.debug("propMap: " + propMap);
                    }
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> settingMap = (Map<String, Object>) targetMap.get("settings");
                    riverConfig.addScrapingRule(settingMap, patternMap, propMap);
                } else {
                    logger.warn("Invalid pattern or target: patternMap: " + patternMap + ", propMap: " + propMap);
                }
            }

            // run s2robot
            s2Robot.execute();

            s2Robot.stop();

        } finally {
            // invoke finish event script
            executeScript(crawlSettings, vars, "finish");

            if (cleanup) {
                s2Robot.cleanup(sessionId);
            }
        }

        return 0;
    }

    protected void executeScript(final Map<String, Object> crawlSettings, final Map<String, Object> vars, final String target) {
        final Map<String, Object> scriptSettings = SettingsUtils.get(crawlSettings, "script");
        final String script = SettingsUtils.get(scriptSettings, target);
        final String lang = SettingsUtils.get(scriptSettings, "lang", WebRiverConstants.DEFAULT_SCRIPT_LANG);
        final String scriptTypeValue = SettingsUtils.get(scriptSettings, "script_type", "inline");
        ScriptType scriptType;
        if (ScriptType.FILE.toString().equalsIgnoreCase(scriptTypeValue)) {
            scriptType = ScriptType.FILE;
        } else if (ScriptType.INDEXED.toString().equalsIgnoreCase(scriptTypeValue)) {
            scriptType = ScriptType.INDEXED;
        } else {
            scriptType = ScriptType.INLINE;
        }
        if (StringUtil.isNotBlank(script)) {
            final Map<String, Object> localVars = new HashMap<String, Object>(vars);
            localVars.put("container", SingletonLaContainerFactory.getContainer());
            localVars.put("settings", crawlSettings);
            localVars.put("logger", logger);
            try {
                final Object result = scriptService.execute(lang, script, scriptType, localVars);
                logger.info("[{}] \"{}\" => {}", target, script, result);
            } catch (final Exception e) {
                logger.warn("Failed to execute script: {}", e, script);
            }
        }
    }

}
