package org.codelibs.riverweb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.stream.Stream;

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
import org.codelibs.fess.crawler.Crawler;
import org.codelibs.fess.crawler.CrawlerContext;
import org.codelibs.fess.crawler.client.CrawlerClient;
import org.codelibs.fess.crawler.client.CrawlerClientFactory;
import org.codelibs.fess.crawler.client.http.Authentication;
import org.codelibs.fess.crawler.client.http.HcHttpClient;
import org.codelibs.fess.crawler.client.http.RequestHeader;
import org.codelibs.fess.crawler.client.http.impl.AuthenticationImpl;
import org.codelibs.fess.crawler.client.http.ntlm.JcifsEngine;
import org.codelibs.fess.crawler.service.impl.EsDataService;
import org.codelibs.fess.crawler.service.impl.EsUrlFilterService;
import org.codelibs.fess.crawler.service.impl.EsUrlQueueService;
import org.codelibs.riverweb.app.service.ScriptService;
import org.codelibs.riverweb.config.RiverConfig;
import org.codelibs.riverweb.config.RiverConfigManager;
import org.codelibs.riverweb.interval.WebRiverIntervalController;
import org.codelibs.riverweb.util.ConfigProperties;
import org.codelibs.riverweb.util.SettingsUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
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

    @Option(name = "--queue-timeout")
    protected long queueTimeout = 300000; // 5min

    @Option(name = "--threads")
    protected int numThreads = 1;

    @Option(name = "--interval")
    protected long interval = 1000;

    @Option(name = "--config-id")
    protected String configId;

    @Option(name = "--session-id")
    protected String sessionId;

    @Option(name = "--cleanup")
    protected boolean cleanup;

    @Option(name = "--es-hosts")
    protected String esHosts;

    @Option(name = "--cluster-name")
    protected String clusterName;

    @Option(name = "--quiet")
    protected boolean quiet;

    @Resource
    protected org.codelibs.fess.crawler.client.EsClient esClient;

    @Resource
    protected ConfigProperties config;

    @Resource
    protected ScriptService scriptService;

    @Resource
    protected RiverConfigManager riverConfigManager;

    @Resource
    protected String defaultUserAgent;

    protected static IntConsumer exitMethod = System::exit;

    public static void main(final String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                synchronized (this) {
                    SingletonLaContainerFactory.destroy();
                }
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

        try {
            exitMethod.accept(riverWeb.execute());
        } catch (final Exception e) {
            riverWeb.print(e.getMessage());
            exitMethod.accept(1);
            logger.error("Failed to process your request.", e);
        } finally {
            SingletonLaContainerFactory.destroy();
        }
    }

    private void print(final String format, final Object... args) {
        final String log = String.format(format, args);
        if (quiet) {
            logger.info(log);
        } else {
            System.out.println(log);
        }
    }

    private int execute() {
        // update esClient
        esClient.setClusterName(config.getElasticsearchClusterName(clusterName));
        esClient.setAddresses(config.getElasticsearchHosts(esHosts));
        esClient.connect();

        if (StringUtil.isNotBlank(configId)) {
            return crawl(SingletonLaContainer.getComponent(Crawler.class), configId, sessionId);
        } else {
            final String configIndex = config.getConfigIndex();
            final String queueType = config.getQueueType();
            final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
            final Future<?>[] results = new Future[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i + 1;
                results[i] = threadPool.submit(() -> {
                    AtomicLong lastProcessed = new AtomicLong(System.currentTimeMillis());
                    while (SingletonLaContainerFactory.hasContainer()
                            && (queueTimeout <= 0 || lastProcessed.get() + queueTimeout > System.currentTimeMillis())) {
                        logger.debug("Checking queue: {}/{}", configIndex, queueType);
                        try {
                            esClient.prepareSearch(configIndex).setTypes(queueType)
                                    .setQuery(
                                            QueryBuilders.functionScoreQuery().add(ScoreFunctionBuilders.randomFunction(System.nanoTime())))
                                    .setSize(config.getQueueParsingSize()).execute().actionGet().getHits().forEach(hit -> {
                                if (esClient.prepareDelete(hit.getIndex(), hit.getType(), hit.getId()).execute().actionGet().isFound()) {
                                    Map<String, Object> source = hit.getSource();
                                    final Object configId = source.get("config_id");
                                    final String sessionId = (String) source.get("session_id");
                                    if (configId instanceof String) {
                                        print("Config %s is started with Session %s.", configId, sessionId);
                                        try {
                                            crawl(SingletonLaContainer.getComponent(Crawler.class), configId.toString(), sessionId);
                                        } finally {
                                            print("Config %s is finished.", configId);
                                            lastProcessed.set(System.currentTimeMillis());
                                        }
                                    }
                                } else if (logger.isDebugEnabled()) {
                                    logger.debug("No data in queue: " + hit.getIndex() + "/" + hit.getType() + "/" + hit.getId());
                                }
                            });
                        } catch (IndexNotFoundException e) {
                            logger.debug("Index is not found.", e);
                        } catch (Exception e) {
                            logger.warn("Failed to process a queue.", e);
                        }
                        try {
                            Thread.sleep(interval);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    print("Thread %d is finished.", threadId);
                });
            }
            Stream.of(results).forEach(f -> {
                try {
                    f.get();
                } catch (Exception e) {
                    // ignore
                }
            });
            threadPool.shutdown();
            return 0;
        }
    }

    private int crawl(Crawler crawler, String configId, String sessionId) {
        // Load config data
        final String configIndex = config.getConfigIndex();
        final String configType = config.getConfigType();
        final GetResponse response = esClient.prepareGet(configIndex, configType, configId).execute().actionGet();
        if (!response.isExists()) {
            print("Config ID %s is not found in %s/%s.", configId, configIndex, configType);
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

        final RiverConfig riverConfig = riverConfigManager.get(sessionId);
        try {
            // invoke execute event script
            executeScript(crawlSettings, vars, "execute");

            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> targetList = (List<Map<String, Object>>) crawlSettings.get("target");
            if (targetList == null || targetList.isEmpty()) {
                print("No targets for crawling.");
                return 1;
            }

            crawler.setSessionId(sessionId);

            // HttpClient Parameters
            final Map<String, Object> paramMap = new HashMap<String, Object>();
            final CrawlerClientFactory clientFactory = crawler.getClientFactory();

            final Integer connectionTimeout = SettingsUtils.get(crawlSettings, "connection_timeout", config.getConnectionTimeout());
            if (connectionTimeout != null) {
                paramMap.put(HcHttpClient.CONNECTION_TIMEOUT_PROPERTY, connectionTimeout);
            }

            final Integer soTimeout = SettingsUtils.get(crawlSettings, "so_timeout", config.getSoTimeout());
            if (soTimeout != null) {
                paramMap.put(HcHttpClient.SO_TIMEOUT_PROPERTY, soTimeout);
            }

            // web driver
            @SuppressWarnings("unchecked")
            final List<String> wdUrlList = (List<String>) crawlSettings.get("web_driver_urls");
            if (wdUrlList != null) {
                CrawlerClient client = SingletonLaContainer.getComponent("webDriverClient");
                wdUrlList.stream().forEach(regex -> clientFactory.addClient(regex, client, 0));
            }

            clientFactory.setInitParameterMap(paramMap);

            // user agent
            final String userAgent = SettingsUtils.get(crawlSettings, "user_agent", defaultUserAgent);
            if (StringUtil.isNotBlank(userAgent)) {
                paramMap.put(HcHttpClient.USER_AGENT_PROPERTY, userAgent);
            }

            // robots.txt parser
            final Boolean robotsTxtEnabled = SettingsUtils.get(crawlSettings, "robots_txt", config.isRobotsTxtEnabled());
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
                        credentials = new NTCredentials(username, password, workstation == null ? StringUtil.EMPTY : workstation,
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
            final List<String> urlList = (List<String>) crawlSettings.get("urls");
            if (urlList == null || urlList.isEmpty()) {
                print("No url for crawling.");
                return 1;
            }
            for (final String url : urlList) {
                try {
                    crawler.addUrl(url);
                } catch (DocumentAlreadyExistsException e) {
                    logger.warn(url + " exists in " + sessionId);
                }
            }
            // include regex
            @SuppressWarnings("unchecked")
            final List<String> includeFilterList = (List<String>) crawlSettings.get("include_urls");
            if (includeFilterList != null) {
                for (final String regex : includeFilterList) {
                    try {
                        crawler.addIncludeFilter(regex);
                    } catch (DocumentAlreadyExistsException e) {
                        logger.warn(regex + " exists in " + sessionId);
                    }
                }
            }
            // exclude regex
            @SuppressWarnings("unchecked")
            final List<String> excludeFilterList = (List<String>) crawlSettings.get("exclude_urls");
            if (excludeFilterList != null) {
                for (final String regex : excludeFilterList) {
                    try {
                        crawler.addExcludeFilter(regex);
                    } catch (DocumentAlreadyExistsException e) {
                        logger.warn(regex + " exists in " + sessionId);
                    }
                }
            }

            final CrawlerContext robotContext = crawler.getCrawlerContext();

            // max depth
            final int maxDepth = SettingsUtils.get(crawlSettings, "max_depth", -1);

            robotContext.setMaxDepth(maxDepth);
            // max access count
            final int maxAccessCount = SettingsUtils.get(crawlSettings, "max_access_count", 100);
            robotContext.setMaxAccessCount(maxAccessCount);
            // num of thread
            final int numOfThread = SettingsUtils.get(crawlSettings, "num_of_thread", 5);
            robotContext.setNumOfThread(numOfThread);
            // interval
            final long interval = SettingsUtils.get(crawlSettings, "interval", 1000L);
            final WebRiverIntervalController intervalController = (WebRiverIntervalController) crawler.getIntervalController();
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
            crawler.execute();

            crawler.stop();

        } finally {
            // invoke finish event script
            executeScript(crawlSettings, vars, "finish");
            riverConfigManager.remove(sessionId);

            if (cleanup) {
                final EsUrlFilterService urlFilterService = SingletonLaContainer.getComponent(EsUrlFilterService.class);
                final EsUrlQueueService urlQueueService = SingletonLaContainer.getComponent(EsUrlQueueService.class);
                final EsDataService dataService = SingletonLaContainer.getComponent(EsDataService.class);

                try {
                    // clear url filter
                    urlFilterService.delete(sessionId);
                } catch (Exception e) {
                    logger.warn("Failed to delete UrlFilter for " + sessionId, e);
                }

                try {
                    // clear queue
                    urlQueueService.clearCache();
                    urlQueueService.delete(sessionId);
                } catch (Exception e) {
                    logger.warn("Failed to delete UrlQueue for " + sessionId, e);
                }

                try {
                    // clear
                    dataService.delete(sessionId);
                } catch (Exception e) {
                    logger.warn("Failed to delete AccessResult for " + sessionId, e);
                }
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
