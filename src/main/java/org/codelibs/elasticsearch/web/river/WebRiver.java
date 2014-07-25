package org.codelibs.elasticsearch.web.river;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.time.DateUtils;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.auth.NTLMScheme;
import org.codelibs.elasticsearch.quartz.service.ScheduleService;
import org.codelibs.elasticsearch.util.lang.StringUtils;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;
import org.codelibs.elasticsearch.web.WebRiverConstants;
import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.codelibs.elasticsearch.web.robot.interval.WebRiverIntervalController;
import org.codelibs.elasticsearch.web.robot.service.EsDataService;
import org.codelibs.elasticsearch.web.robot.service.EsUrlFilterService;
import org.codelibs.elasticsearch.web.robot.service.EsUrlQueueService;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.seasar.framework.container.SingletonS2Container;
import org.seasar.framework.container.factory.SingletonS2ContainerFactory;
import org.seasar.framework.util.StringUtil;
import org.seasar.robot.S2Robot;
import org.seasar.robot.S2RobotContext;
import org.seasar.robot.client.http.Authentication;
import org.seasar.robot.client.http.HcHttpClient;
import org.seasar.robot.client.http.RequestHeader;
import org.seasar.robot.client.http.impl.AuthenticationImpl;
import org.seasar.robot.client.http.ntlm.JcifsEngine;

public class WebRiver extends AbstractRiverComponent implements River {

    private static final ESLogger logger = Loggers.getLogger(WebRiver.class);

    private static final String RIVER_NAME = "riverName";

    private static final String SETTINGS = "settings";

    private static final String RUNNING_JOB = "runningJob";

    private static final String SCRIPT_SERVICE = "scriptService";

    private static final String TRIGGER_ID_SUFFIX = "Trigger";

    private static final String JOB_ID_SUFFIX = "Job";

    private static final String ES_CLIENT = "esClient";

    private static final String DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Elasticsearch River Web/"
            + WebRiverConstants.VERSION + ")";

    private static final String NTLM_SCHEME = "NTLM";

    private static final String DIGEST_SCHEME = "DIGEST";

    private static final String BASIC_SCHEME = "BASIC";

    private static final String ONE_TIME = "oneTime";

    private static final String EMPTY_STRING = "";

    private final Client client;

    private final ScheduleService scheduleService;

    private String groupId;

    private String id;

    private AtomicReference<CrawlJob> runningJob = new AtomicReference<CrawlJob>();

    private ScriptService scriptService;

    @Inject
    public WebRiver(final RiverName riverName, final RiverSettings settings,
            final Client client, final ScheduleService scheduleService,
            final ScriptService scriptService) {
        super(riverName, settings);
        this.client = client;
        this.scheduleService = scheduleService;
        this.scriptService = scriptService;

        groupId = riverName.type() == null ? "web" : riverName.type();
        id = riverName.name();

        logger.info("Creating WebRiver: " + id);
    }

    @Override
    public void start() {
        logger.info("Scheduling CrawlJob...");

        if (scheduleService == null) {
            logger.warn("Elasticsearch River Web plugin depends on Elasticsearch Quartz plugin, "
                    + "but it's not found. River Web plugin does not start.");
            return;
        }

        final JobDataMap jobDataMap = new JobDataMap();

        String cron = null;
        @SuppressWarnings("unchecked")
        final Map<String, Object> scheduleSettings = (Map<String, Object>) settings
                .settings().get("schedule");
        if (scheduleSettings != null) {
            cron = (String) scheduleSettings.get("cron");
        }

        if (cron == null) {
            final Date now = new Date();
            DateUtils.addSeconds(now, 60);
            final SimpleDateFormat sdf = new SimpleDateFormat(
                    "s m H d M ? yyyy");
            cron = sdf.format(now);
            jobDataMap.put(ONE_TIME, Boolean.TRUE);
        }

        jobDataMap.put(RIVER_NAME, riverName);
        jobDataMap.put(SETTINGS, settings);
        jobDataMap.put(ES_CLIENT, client);
        jobDataMap.put(RUNNING_JOB, runningJob);
        jobDataMap.put(SCRIPT_SERVICE, scriptService);

        final Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("riverName", riverName);
        vars.put("client", client);
        executeScript(scriptService, settings.settings(), vars, "start");

        final JobDetail crawlJob = newJob(CrawlJob.class)
                .withIdentity(id + JOB_ID_SUFFIX, groupId)
                .usingJobData(jobDataMap).build();

        final Trigger trigger = newTrigger()
                .withIdentity(id + TRIGGER_ID_SUFFIX, groupId)
                .withSchedule(cronSchedule(cron)).startNow().build();
        scheduleService.scheduleJob(crawlJob, trigger);
    }

    @Override
    public void close() {
        if (scheduleService == null) {
            return;
        }

        final Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("riverName", riverName);
        vars.put("client", client);
        executeScript(scriptService, settings.settings(), vars, "close");

        logger.info("Unscheduling  CrawlJob...");

        final CrawlJob crawlJob = runningJob.get();
        if (crawlJob != null) {
            crawlJob.stop();
        }
        scheduleService.deleteJob(jobKey(id + JOB_ID_SUFFIX, groupId));
    }

    protected static void executeScript(final ScriptService scriptService,
            final Map<String, Object> settings, final Map<String, Object> vars,
            final String target) {
        final Map<String, Object> crawlSettings = SettingsUtils.get(settings,
                "crawl");
        final Map<String, Object> scriptSettings = SettingsUtils.get(
                crawlSettings, "script");
        final String script = SettingsUtils.get(scriptSettings, target);
        final String lang = SettingsUtils.get(scriptSettings, "lang");
        final String scriptTypeValue = SettingsUtils.get(scriptSettings,
                "script_type", "inline");
        ScriptType scriptType;
        if (ScriptType.FILE.toString().equalsIgnoreCase(scriptTypeValue)) {
            scriptType = ScriptType.FILE;
        } else if (ScriptType.INDEXED.toString().equalsIgnoreCase(
                scriptTypeValue)) {
            scriptType = ScriptType.INDEXED;
        } else {
            scriptType = ScriptType.INLINE;
        }
        if (StringUtils.isNotBlank(script)) {
            final Map<String, Object> localVars = new HashMap<String, Object>(
                    vars);
            localVars.put("container",
                    SingletonS2ContainerFactory.getContainer());
            localVars.put("settings", settings);
            try {
                final CompiledScript compiledScript = scriptService.compile(
                        lang, script, scriptType);
                logger.info("[{}] \"{}\" => {}", target, script,
                        scriptService.execute(compiledScript, localVars));
            } catch (final Exception e) {
                logger.warn("Failed to execute script: {}", e, script);
            }
        }
    }

    public static class CrawlJob implements Job {

        private S2Robot s2Robot;

        @Override
        public void execute(final JobExecutionContext context)
                throws JobExecutionException {

            final JobDataMap data = context.getMergedJobDataMap();
            @SuppressWarnings("unchecked")
            final AtomicReference<Job> runningJob = (AtomicReference<Job>) data
                    .get(RUNNING_JOB);
            final ScriptService scriptSerivce = (ScriptService) data
                    .get(SCRIPT_SERVICE);
            if (!runningJob.compareAndSet(null, this)) {
                logger.info(context.getJobDetail().getKey() + " is running.");
                return;
            }

            final RiverName riverName = (RiverName) data.get(RIVER_NAME);
            final String sessionId = UUID.randomUUID().toString();

            final Client client = getClient(data);
            final Map<String, Object> vars = new HashMap<String, Object>();
            vars.put("riverName", riverName);
            vars.put("sessionId", sessionId);
            vars.put("client", client);

            RiverConfig riverConfig = null;
            final RiverSettings settings = (RiverSettings) data.get(SETTINGS);
            final Map<String, Object> rootSettings = settings.settings();
            try {

                executeScript(scriptSerivce, rootSettings, vars, "execute");

                @SuppressWarnings("unchecked")
                final Map<String, Object> crawlSettings = (Map<String, Object>) rootSettings
                        .get("crawl");
                if (crawlSettings == null) {
                    logger.warn("No settings for crawling.");
                    return;
                }

                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> targetList = (List<Map<String, Object>>) crawlSettings
                        .get("target");
                if (targetList == null || targetList.isEmpty()) {
                    logger.warn("No targets for crawling.");
                    return;
                }

                s2Robot = SingletonS2Container.getComponent(S2Robot.class);
                s2Robot.setSessionId(sessionId);

                // HttpClient Parameters
                final Map<String, Object> paramMap = new HashMap<String, Object>();
                s2Robot.getClientFactory().setInitParameterMap(paramMap);

                // user agent
                final String userAgent = SettingsUtils.get(crawlSettings,
                        "userAgent", DEFAULT_USER_AGENT);
                if (StringUtil.isNotBlank(userAgent)) {
                    paramMap.put(HcHttpClient.USER_AGENT_PROPERTY, userAgent);
                }

                // robots.txt parser
                final Boolean robotsTxtEnabled = SettingsUtils.get(
                        crawlSettings, "robotsTxt", Boolean.TRUE);
                paramMap.put(HcHttpClient.ROBOTS_TXT_ENABLED_PROPERTY,
                        robotsTxtEnabled);

                // proxy
                final Map<String, Object> proxyMap = SettingsUtils.get(
                        crawlSettings, "proxy", null);
                if (proxyMap != null) {
                    final Object host = proxyMap.get("host");
                    if (host != null) {
                        paramMap.put(HcHttpClient.PROXY_HOST_PROPERTY, host);
                        final Object portObj = proxyMap.get("port");
                        if (portObj instanceof Integer) {
                            paramMap.put(HcHttpClient.PROXY_PORT_PROPERTY,
                                    portObj);
                        } else {
                            paramMap.put(HcHttpClient.PROXY_PORT_PROPERTY,
                                    Integer.valueOf(8080));
                        }
                    }
                }

                // authentications
                // "authentications":[{"scope":{"scheme":"","host":"","port":0,"realm":""},
                //   "credentials":{"username":"","password":""}},{...}]
                final List<Map<String, Object>> authList = SettingsUtils.get(
                        crawlSettings, "authentications", null);
                if (authList != null && !authList.isEmpty()) {
                    final List<Authentication> basicAuthList = new ArrayList<Authentication>();
                    for (final Map<String, Object> authObj : authList) {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> scopeMap = (Map<String, Object>) authObj
                                .get("scope");
                        String scheme = SettingsUtils.get(scopeMap, "scheme",
                                EMPTY_STRING).toUpperCase(Locale.ENGLISH);
                        if (StringUtil.isBlank(scheme)) {
                            logger.warn("Invalid authentication: " + authObj);
                            continue;
                        }
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> credentialMap = (Map<String, Object>) authObj
                                .get("credentials");
                        final String username = SettingsUtils.get(
                                credentialMap, "username", null);
                        if (StringUtil.isBlank(username)) {
                            logger.warn("Invalid authentication: " + authObj);
                            continue;
                        }
                        final String host = SettingsUtils.get(authObj, "host",
                                AuthScope.ANY_HOST);
                        final int port = SettingsUtils.get(authObj, "port",
                                AuthScope.ANY_PORT);
                        final String realm = SettingsUtils.get(authObj,
                                "realm", AuthScope.ANY_REALM);
                        final String password = SettingsUtils.get(
                                credentialMap, "password", null);

                        AuthScheme authScheme = null;
                        Credentials credentials = null;
                        if (BASIC_SCHEME.equalsIgnoreCase(scheme)) {
                            authScheme = new BasicScheme();
                            credentials = new UsernamePasswordCredentials(
                                    username, password);
                        } else if (DIGEST_SCHEME.equals(scheme)) {
                            authScheme = new DigestScheme();
                            credentials = new UsernamePasswordCredentials(
                                    username, password);
                        } else if (NTLM_SCHEME.equals(scheme)) {
                            authScheme = new NTLMScheme(new JcifsEngine());
                            scheme = AuthScope.ANY_SCHEME;
                            final String workstation = SettingsUtils.get(
                                    credentialMap, "workstation", null);
                            final String domain = SettingsUtils.get(
                                    credentialMap, "domain", null);
                            credentials = new NTCredentials(username, password,
                                    workstation == null ? EMPTY_STRING
                                            : workstation,
                                    domain == null ? EMPTY_STRING : domain);
                        }

                        final AuthenticationImpl auth = new AuthenticationImpl(
                                new AuthScope(host, port, realm, scheme),
                                credentials, authScheme);
                        basicAuthList.add(auth);
                    }
                    paramMap.put(HcHttpClient.BASIC_AUTHENTICATIONS_PROPERTY,
                            basicAuthList
                                    .toArray(new Authentication[basicAuthList
                                            .size()]));
                }

                // request header
                // "headers":[{"name":"","value":""},{}]
                final List<Map<String, Object>> headerList = SettingsUtils.get(
                        crawlSettings, "headers", null);
                if (headerList != null && !headerList.isEmpty()) {
                    final List<RequestHeader> requestHeaderList = new ArrayList<RequestHeader>();
                    for (final Map<String, Object> headerObj : headerList) {
                        final String name = SettingsUtils.get(headerObj,
                                "name", null);
                        final String value = SettingsUtils.get(headerObj,
                                "value", null);
                        if (name != null && value != null) {
                            requestHeaderList
                                    .add(new RequestHeader(name, value));
                        }
                    }
                    paramMap.put(
                            HcHttpClient.REQUERT_HEADERS_PROPERTY,
                            requestHeaderList
                                    .toArray(new RequestHeader[requestHeaderList
                                            .size()]));
                }

                // url
                @SuppressWarnings("unchecked")
                final List<String> urlList = (List<String>) crawlSettings
                        .get("url");
                if (urlList == null || urlList.isEmpty()) {
                    logger.warn("No url for crawling.");
                    return;
                }
                for (final String url : urlList) {
                    s2Robot.addUrl(url);
                }
                // include regex
                @SuppressWarnings("unchecked")
                final List<String> includeFilterList = (List<String>) crawlSettings
                        .get("includeFilter");
                if (includeFilterList != null) {
                    for (final String regex : includeFilterList) {
                        s2Robot.addIncludeFilter(regex);
                    }
                }
                // exclude regex
                @SuppressWarnings("unchecked")
                final List<String> excludeFilterList = (List<String>) crawlSettings
                        .get("excludeFilter");
                if (excludeFilterList != null) {
                    for (final String regex : excludeFilterList) {
                        s2Robot.addExcludeFilter(regex);
                    }
                }

                final S2RobotContext robotContext = s2Robot.getRobotContext();

                // max depth
                final int maxDepth = SettingsUtils.get(crawlSettings,
                        "maxDepth", -1);

                robotContext.setMaxDepth(maxDepth);
                // max access count
                final int maxAccessCount = SettingsUtils.get(crawlSettings,
                        "maxAccessCount", 100);
                robotContext.setMaxAccessCount(maxAccessCount);
                // num of thread
                final int numOfThread = SettingsUtils.get(crawlSettings,
                        "numOfThread", 5);
                robotContext.setNumOfThread(numOfThread);
                // interval
                final long interval = SettingsUtils.get(crawlSettings,
                        "interval", 1000);
                final WebRiverIntervalController intervalController = (WebRiverIntervalController) s2Robot
                        .getIntervalController();
                intervalController.setDelayMillisForWaitingNewUrl(interval);

                // river params
                final Map<String, Object> riverParamMap = new HashMap<String, Object>();
                riverParamMap.put("index",
                        SettingsUtils.get(crawlSettings, "index", "web"));
                riverParamMap.put(
                        "type",
                        SettingsUtils.get(crawlSettings, "type",
                                riverName.getName()));
                riverParamMap.put("overwrite", SettingsUtils.get(crawlSettings,
                        "overwrite", Boolean.FALSE));
                riverParamMap.put("incremental", SettingsUtils.get(
                        crawlSettings, "incremental", Boolean.FALSE));

                // crawl config
                riverConfig = SingletonS2Container
                        .getComponent(RiverConfig.class);
                riverConfig.createLock(sessionId);
                riverConfig.addRiverParams(sessionId, riverParamMap);
                for (final Map<String, Object> targetMap : targetList) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> patternMap = (Map<String, Object>) targetMap
                            .get("pattern");
                    @SuppressWarnings("unchecked")
                    final Map<String, Map<String, Object>> propMap = (Map<String, Map<String, Object>>) targetMap
                            .get("properties");
                    if (patternMap != null && propMap != null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("patternMap: " + patternMap);
                            logger.debug("propMap: " + propMap);
                        }
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> settingMap = (Map<String, Object>) targetMap
                                .get("settings");
                        riverConfig.addScrapingRule(sessionId, settingMap,
                                patternMap, propMap);
                    } else {
                        logger.warn("Invalid pattern or target: patternMap: "
                                + patternMap + ", propMap: " + propMap);
                    }
                }

                // run s2robot
                s2Robot.execute();

                s2Robot.stop();

            } finally {
                executeScript(scriptSerivce, rootSettings, vars, "finish");

                runningJob.set(null);
                if (riverConfig != null) {
                    riverConfig.cleanup(sessionId);
                }
                // clean up
                // s2Robot.cleanup(sessionId);
                try {
                    SingletonS2Container.getComponent(EsUrlQueueService.class)
                            .delete(sessionId);
                } catch (final Exception e) {
                    logger.warn("Failed to delete ", e);
                }
                SingletonS2Container.getComponent(EsDataService.class).delete(
                        sessionId);
                SingletonS2Container.getComponent(EsUrlFilterService.class)
                        .delete(sessionId);

                final Object oneTime = data.get(ONE_TIME);
                if (oneTime != null) {
                    if (client != null) {
                        final DeleteMappingResponse deleteMappingResponse = client
                                .admin().indices()
                                .prepareDeleteMapping("_river")
                                .setType(riverName.name()).execute()
                                .actionGet();
                        if (deleteMappingResponse.isAcknowledged()) {
                            logger.info("Deleted one time river: "
                                    + riverName.name());
                        } else {
                            logger.warn("Failed to delete " + riverName.name()
                                    + ". Resposne: "
                                    + deleteMappingResponse.toString());
                        }
                    }
                }
            }
        }

        private Client getClient(final JobDataMap data) {
            final Object clientObj = data.get(ES_CLIENT);
            if (clientObj instanceof Client) {
                return (Client) clientObj;
            }
            return null;
        }

        public void stop() {
            if (s2Robot != null) {
                s2Robot.stop();
            }
        }

    }

}
