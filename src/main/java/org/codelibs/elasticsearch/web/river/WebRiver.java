package org.codelibs.elasticsearch.web.river;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.web.service.S2ContainerService;
import org.codelibs.elasticsearch.web.service.ScheduleService;
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
import org.seasar.framework.container.S2Container;
import org.seasar.robot.S2Robot;
import org.seasar.robot.entity.AccessResult;
import org.seasar.robot.service.DataService;
import org.seasar.robot.util.AccessResultCallback;

public class WebRiver extends AbstractRiverComponent implements River {
    private static final String S2_CONTAINER = "s2Container";

    private static final String TRIGGER_ID_SUFFIX = "Trigger";

    private static final String JOB_ID_SUFFIX = "Job";

    private static final String ES_CLIENT = "esClient";

    private final Client client;

    private final ScheduleService scheduleService;

    private S2ContainerService containerService;

    private String groupId;

    private String id;

    @Inject
    public WebRiver(final RiverName riverName, final RiverSettings settings,
            final Client client, final ScheduleService scheduleService,
            final S2ContainerService containerService) {
        super(riverName, settings);
        this.client = client;
        this.scheduleService = scheduleService;
        this.containerService = containerService;

        groupId = riverName.type() == null ? "web" : riverName.type();
        id = riverName.name();

        logger.info("Creating WebRiver: " + id);
    }

    @Override
    public void start() {
        logger.info("Scheduling CrawlJob...");

        final JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("settings", settings);
        jobDataMap.put(ES_CLIENT, client);
        jobDataMap.put(S2_CONTAINER, containerService.getContainer());
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
        private static final ESLogger logger = Loggers
                .getLogger(CrawlJob.class);

        @Override
        public void execute(final JobExecutionContext context)
                throws JobExecutionException {

            final JobDataMap data = context.getMergedJobDataMap();
            Client client = (Client) data.get(ES_CLIENT);
            S2Container container = (S2Container) data.get(S2_CONTAINER);
            RiverSettings settings = (RiverSettings) data.get("settings");
            @SuppressWarnings("unchecked")
            Map<String, Object> crawlSettings = (Map<String, Object>) settings
                    .settings().get("crawl");
            if (crawlSettings == null) {
                logger.warn("No settings for crawling.");
                return;
            }

            S2Robot s2Robot = (S2Robot) container.getComponent(S2Robot.class);
            // add url
            @SuppressWarnings("unchecked")
            List<String> urlList = (List<String>) crawlSettings.get("url");
            if (urlList == null || urlList.isEmpty()) {
                logger.warn("No url for crawling.");
                return;
            }
            for (String url : urlList) {
                s2Robot.addUrl(url);
            }
            // depth
            int maxDepth = getInitParameter(crawlSettings, "maxDepth", -1);
            s2Robot.getRobotContext().setMaxDepth(maxDepth);

            // run s2robot
            String sessionId = s2Robot.execute();

            // print urls
            DataService dataService = (DataService) container
                    .getComponent(DataService.class);
            logger.info("Crawled URLs: ");
            dataService.iterate(sessionId, new AccessResultCallback() {
                public void iterate(AccessResult accessResult) {
                    logger.info(accessResult.getUrl());
                }
            });

            // clean up
            // s2Robot.cleanup(sessionId);

        }

        @SuppressWarnings("unchecked")
        private <T> T getInitParameter(Map<String, Object> crawlSettings,
                String key, T defaultValue) {
            Object value = crawlSettings.get(key);
            if (value != null) {
                return (T) value;
            }
            return defaultValue;
        }
    }
}
