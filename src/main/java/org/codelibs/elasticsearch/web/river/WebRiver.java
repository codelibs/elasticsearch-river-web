package org.codelibs.elasticsearch.web.river;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.codelibs.elasticsearch.web.service.ScheduleService;
import org.codelibs.elasticsearch.web.transformer.ScrapingTransformer;
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
import org.seasar.robot.S2Robot;
import org.seasar.robot.processor.impl.DefaultResponseProcessor;
import org.seasar.robot.rule.RuleManager;
import org.seasar.robot.rule.impl.RegexRule;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WebRiver extends AbstractRiverComponent implements River {
    private static final ESLogger logger = Loggers.getLogger(WebRiver.class);

    private static final String SETTINGS = "settings";

    private static final String IS_RUNNING = "isRunning";

    private static final String TRIGGER_ID_SUFFIX = "Trigger";

    private static final String JOB_ID_SUFFIX = "Job";

    private static final String ES_CLIENT = "esClient";

    private final Client client;

    private final ScheduleService scheduleService;

    private String groupId;

    private String id;

    private AtomicBoolean isRunning = new AtomicBoolean(false);

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

        final JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(SETTINGS, settings);
        jobDataMap.put(ES_CLIENT, client);
        jobDataMap.put(IS_RUNNING, isRunning);
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

            scheduleService.schedule(crawlJob, trigger);
        }
    }

    @Override
    public void close() {
        logger.info("Unscheduling  CrawlJob...");

        scheduleService.unschedule(groupId, id + JOB_ID_SUFFIX);
    }

    public static class CrawlJob implements Job {

        @Override
        public void execute(final JobExecutionContext context)
                throws JobExecutionException {

            final JobDataMap data = context.getMergedJobDataMap();
            final AtomicBoolean isRunning = (AtomicBoolean) data
                    .get(IS_RUNNING);
            if (isRunning.getAndSet(true)) {
                return;
            }

            try {
                final Client client = (Client) data.get(ES_CLIENT);
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

                final ObjectMapper objectMapper = SingletonS2Container
                        .getComponent(ObjectMapper.class);
                final S2Robot s2Robot = SingletonS2Container
                        .getComponent(S2Robot.class);
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
                // max depth
                final int maxDepth = getInitParameter(crawlSettings,
                        "maxDepth", -1);
                s2Robot.getRobotContext().setMaxDepth(maxDepth);
                // max access count
                final int maxAccessCount = getInitParameter(crawlSettings,
                        "maxAccessCount", 100);
                s2Robot.getRobotContext().setMaxAccessCount(maxAccessCount);

                // crawl config
                final ScrapingTransformer transformer = new ScrapingTransformer();
                transformer.setClient(client);
                transformer.setIndexName(getInitParameter(crawlSettings,
                        "index", "web"));
                transformer.setObjectMapper(objectMapper);
                for (final Map<String, Object> targetMap : targetList) {
                    final String urlPattern = (String) targetMap
                            .get("urlPattern");
                    @SuppressWarnings("unchecked")
                    final Map<String, String> propMap = (Map<String, String>) targetMap
                            .get("properties");
                    if (urlPattern != null && propMap != null) {
                        transformer.addScrapingRule(
                                Pattern.compile(urlPattern), propMap);
                    }
                }
                final DefaultResponseProcessor responseProcessor = new DefaultResponseProcessor();
                responseProcessor.setTransformer(transformer);
                final RegexRule scrapingRule = new RegexRule();
                scrapingRule.setRuleId("scraping");
                scrapingRule.setDefaultRule(true);
                scrapingRule.setResponseProcessor(responseProcessor);
                final RuleManager ruleManager = SingletonS2Container
                        .getComponent(RuleManager.class);
                ruleManager.addRule(scrapingRule);

                // run s2robot
                final String sessionId = s2Robot.execute();

                // clean up
                s2Robot.cleanup(sessionId);
            } finally {
                isRunning.set(false);
            }
        }

        @SuppressWarnings("unchecked")
        private <T> T getInitParameter(final Map<String, Object> crawlSettings,
                final String key, final T defaultValue) {
            final Object value = crawlSettings.get(key);
            if (value != null) {
                return (T) value;
            }
            return defaultValue;
        }
    }
}
