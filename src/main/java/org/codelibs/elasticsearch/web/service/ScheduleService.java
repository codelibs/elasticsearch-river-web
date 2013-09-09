package org.codelibs.elasticsearch.web.service;

import static org.quartz.JobKey.jobKey;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

public class ScheduleService extends
        AbstractLifecycleComponent<ScheduleService> {
    private Scheduler scheduler;

    private boolean isPause = false;

    @Inject
    public ScheduleService(final Settings settings) {
        super(settings);
        logger.info("Creating Scheduler...");

        final SchedulerFactory sf = new StdSchedulerFactory();
        try {
            scheduler = sf.getScheduler();
        } catch (final SchedulerException e) {
            throw new ElasticSearchException("Failed to create Scheduler.", e);
        }
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        logger.info("Starting Scheduler...");

        try {
            if (isPause) {
                scheduler.resumeAll();
            } else {
                scheduler.start();
            }
        } catch (final SchedulerException e) {
            throw new ElasticSearchException("Failed to start Scheduler.", e);
        }

    }

    @Override
    protected void doStop() throws ElasticSearchException {
        logger.info("Stopping Scheduler...");

        try {
            scheduler.pauseAll();
            isPause = true;
        } catch (final SchedulerException e) {
            throw new ElasticSearchException("Failed to stop Scheduler.", e);
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        logger.info("Closing Scheduler...");

        try {
            scheduler.shutdown(true);
        } catch (final SchedulerException e) {
            throw new ElasticSearchException("Failed to shutdown Scheduler.", e);
        }
    }

    public void schedule(final JobDetail crawlJob, final Trigger trigger) {
        try {
            scheduler.scheduleJob(crawlJob, trigger);
        } catch (final SchedulerException e) {
            throw new ElasticSearchException("Failed to add Job: " + crawlJob
                    + ":" + trigger, e);
        }
    }

    public void unschedule(final String groupId, final String jobId) {
        try {
            scheduler.deleteJob(jobKey(jobId, groupId));
        } catch (final SchedulerException e) {
            throw new ElasticSearchException("Failed to delete Job: " + groupId
                    + ":" + jobId, e);
        }
    }

}
