package org.codelibs.elasticsearch.web.river;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.codelibs.elasticsearch.quartz.service.ScheduleService;
import org.codelibs.elasticsearch.web.WebRiverConstants;
import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.codelibs.elasticsearch.web.robot.interval.WebRiverIntervalController;
import org.codelibs.elasticsearch.web.robot.service.EsDataService;
import org.codelibs.elasticsearch.web.robot.service.EsUrlFilterService;
import org.codelibs.elasticsearch.web.robot.service.EsUrlQueueService;
import org.codelibs.elasticsearch.web.util.ParameterUtil;
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
import org.seasar.robot.client.http.HcHttpClient;

public class WebRiver extends AbstractRiverComponent implements River {
    private static final String RIVER_NAME = "riverName";

    private static final ESLogger logger = Loggers.getLogger(WebRiver.class);

    private static final String SETTINGS = "settings";

    private static final String RUNNING_JOB = "runningJob";

    private static final String TRIGGER_ID_SUFFIX = "Trigger";

    private static final String JOB_ID_SUFFIX = "Job";

    private static final String ES_CLIENT = "esClient";

    private static final String DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Elasticsearch River Web/"
            + WebRiverConstants.VERSION + ")";

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
        jobDataMap.put(RIVER_NAME, riverName);
        jobDataMap.put(SETTINGS, settings);
        jobDataMap.put(ES_CLIENT, client);
        jobDataMap.put(RUNNING_JOB, runningJob);
        final JobDetail crawlJob = newJob(CrawlJob.class)
                .withIdentity(id + JOB_ID_SUFFIX, groupId)
                .usingJobData(jobDataMap).build();

        String cron = null;
        @SuppressWarnings("unchecked")
        final Map<String, Object> scheduleSettings = (Map<String, Object>) settings
                .settings().get("schedule");
        if (scheduleSettings != null) {
            cron = (String) scheduleSettings.get("cron");
        }

        if (cron != null) {
            final Trigger trigger = newTrigger()
                    .withIdentity(id + TRIGGER_ID_SUFFIX, groupId)
                    .withSchedule(cronSchedule(cron)).startNow().build();

            scheduleService.scheduleJob(crawlJob, trigger);
        }
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

                // TODO authentications
                // TODO request headers

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
                riverParamMap.put("type", riverName.getName());
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
                        riverConfig.addScrapingRule(sessionId, patternMap,
                                propMap);
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
            }
        }

        public void stop() {
            if (s2Robot != null) {
                s2Robot.stop();
            }
        }

    }

}
