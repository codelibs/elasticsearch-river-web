package org.codelibs.elasticsearch.web.river;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

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
            logger.info("client: " + data.get(ES_CLIENT));
            logger.info("container: " + data.get(S2_CONTAINER));
        }

    }
}
