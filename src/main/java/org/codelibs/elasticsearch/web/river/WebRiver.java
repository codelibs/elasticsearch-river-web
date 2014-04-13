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
import org.codelibs.elasticsearch.web.WebRiverConstants;
import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.codelibs.elasticsearch.web.robot.interval.WebRiverIntervalController;
import org.codelibs.elasticsearch.web.robot.service.EsDataService;
import org.codelibs.elasticsearch.web.robot.service.EsUrlFilterService;
import org.codelibs.elasticsearch.web.robot.service.EsUrlQueueService;
import org.codelibs.elasticsearch.web.util.ParameterUtil;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.seasar.framework.container.SingletonS2Container;
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

    @Inject
    public WebRiver(final RiverName riverName, final RiverSettings settings,
            final Client client, final ScheduleService scheduleService) {
        super(riverName, settings);
        this.client = client;
        this.scheduleService = scheduleService;

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
            Date now = new Date();
            DateUtils.addSeconds(now, 60);
            SimpleDateFormat sdf = new SimpleDateFormat("s m H d M ? yyyy");
            cron = sdf.format(now);
            jobDataMap.put(ONE_TIME, Boolean.TRUE);
        }

        jobDataMap.put(RIVER_NAME, riverName);
        jobDataMap.put(SETTINGS, settings);
        jobDataMap.put(ES_CLIENT, client);
        jobDataMap.put(RUNNING_JOB, runningJob);
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

        logger.info("Unscheduling  CrawlJob...");

        final CrawlJob crawlJob = runningJob.get();
        if (crawlJob != null) {
            crawlJob.stop();
        }
        scheduleService.deleteJob(jobKey(id + JOB_ID_SUFFIX, groupId));
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
            if (!runningJob.compareAndSet(null, this)) {
                logger.info(context.getJobDetail().getKey() + " is running.");
                return;
            }

            final RiverName riverName = (RiverName) data.get(RIVER_NAME);
            final String sessionId = UUID.randomUUID().toString();

            RiverConfig riverConfig = null;
            try {
                final RiverSettings settings = (RiverSettings) data
                        .get(SETTINGS);
                @SuppressWarnings("unchecked")
                final Map<String, Object> crawlSettings = (Map<String, Object>) settings
                        .settings().get("crawl");
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
                final String userAgent = ParameterUtil.getValue(crawlSettings,
                        "userAgent", DEFAULT_USER_AGENT);
                if (StringUtil.isNotBlank(userAgent)) {
                    paramMap.put(HcHttpClient.USER_AGENT_PROPERTY, userAgent);
                }

                // robots.txt parser
                final Boolean robotsTxtEnabled = ParameterUtil.getValue(crawlSettings,
                        "robotsTxt", Boolean.TRUE);
                paramMap.put(HcHttpClient.ROBOTS_TXT_ENABLED_PROPERTY, robotsTxtEnabled);

                // proxy
                Map<String, Object> proxyMap = ParameterUtil.getValue(
                        crawlSettings, "proxy", null);
                if (proxyMap != null) {
                    Object host = proxyMap.get("host");
                    if (host != null) {
                        paramMap.put(HcHttpClient.PROXY_HOST_PROPERTY, host);
                        Object portObj = proxyMap.get("port");
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
                final List<Map<String, Object>> authList = ParameterUtil
                        .getValue(crawlSettings, "authentications", null);
                if (authList != null && !authList.isEmpty()) {
                    final List<Authentication> basicAuthList = new ArrayList<Authentication>();
                    for (final Map<String, Object> authObj : authList) {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> scopeMap = (Map<String, Object>) authObj
                                .get("scope");
                        String scheme = ParameterUtil.getValue(scopeMap,
                                "scheme", EMPTY_STRING).toUpperCase(
                                Locale.ENGLISH);
                        if (StringUtil.isBlank(scheme)) {
                            logger.warn("Invalid authentication: " + authObj);
                            continue;
                        }
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> credentialMap = (Map<String, Object>) authObj
                                .get("credentials");
                        final String username = ParameterUtil.getValue(
                                credentialMap, "username", null);
                        if (StringUtil.isBlank(username)) {
                            logger.warn("Invalid authentication: " + authObj);
                            continue;
                        }
                        final String host = ParameterUtil.getValue(authObj,
                                "host", AuthScope.ANY_HOST);
                        final int port = ParameterUtil.getValue(authObj,
                                "port", AuthScope.ANY_PORT);
                        final String realm = ParameterUtil.getValue(authObj,
                                "realm", AuthScope.ANY_REALM);
                        final String password = ParameterUtil.getValue(
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
                            final String workstation = ParameterUtil.getValue(
                                    credentialMap, "workstation", null);
                            final String domain = ParameterUtil.getValue(
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
                final List<Map<String, Object>> headerList = ParameterUtil
                        .getValue(crawlSettings, "headers", null);
                if (headerList != null && !headerList.isEmpty()) {
                    final List<RequestHeader> requestHeaderList = new ArrayList<RequestHeader>();
                    for (final Map<String, Object> headerObj : headerList) {
                        final String name = ParameterUtil.getValue(headerObj,
                                "name", null);
                        final String value = ParameterUtil.getValue(headerObj,
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
                final int maxDepth = ParameterUtil.getValue(crawlSettings,
                        "maxDepth", -1);

                robotContext.setMaxDepth(maxDepth);
                // max access count
                final int maxAccessCount = ParameterUtil.getValue(
                        crawlSettings, "maxAccessCount", 100);
                robotContext.setMaxAccessCount(maxAccessCount);
                // num of thread
                final int numOfThread = ParameterUtil.getValue(crawlSettings,
                        "numOfThread", 5);
                robotContext.setNumOfThread(numOfThread);
                // interval
                final long interval = ParameterUtil.getValue(crawlSettings,
                        "interval", 1000);
                final WebRiverIntervalController intervalController = (WebRiverIntervalController) s2Robot
                        .getIntervalController();
                intervalController.setDelayMillisForWaitingNewUrl(interval);

                // river params
                final Map<String, Object> riverParamMap = new HashMap<String, Object>();
                riverParamMap.put("index",
                        ParameterUtil.getValue(crawlSettings, "index", "web"));
                riverParamMap.put(
                        "type",
                        ParameterUtil.getValue(crawlSettings, "type",
                                riverName.getName()));
                riverParamMap.put("overwrite", ParameterUtil.getValue(
                        crawlSettings, "overwrite", Boolean.FALSE));
                riverParamMap.put("incremental", ParameterUtil.getValue(
                        crawlSettings, "incremental", Boolean.FALSE));

                // crawl config
                riverConfig = SingletonS2Container
                        .getComponent(RiverConfig.class);
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

                Object oneTime = data.get(ONE_TIME);
                if (oneTime != null) {
                    Object clientObj = data.get(ES_CLIENT);
                    if (clientObj instanceof Client) {
                        Client client = (Client) clientObj;
                        DeleteMappingResponse deleteMappingResponse = client
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

        public void stop() {
            if (s2Robot != null) {
                s2Robot.stop();
            }
        }

    }

}
