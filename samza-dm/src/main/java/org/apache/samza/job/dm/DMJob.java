package org.apache.samza.job.dm;

import org.apache.samza.config.Config;
import org.apache.samza.config.DMSchedulerConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;

public class DMJob implements StreamJob {

    Config config;

    public DMJob(Config config) {
        this.config = config;
    }

    @Override
    public StreamJob submit() {
        DMSchedulerConfig schedulerConfig = new DMSchedulerConfig(config);
        DMScheduler scheduler = getDMScheduler(schedulerConfig.getSchedulerClass());
        scheduler.init(config, schedulerConfig);
        scheduler.submitApplication();
        return this;
    }

    @Override
    public StreamJob kill() {
        return this;
    }

    @Override
    public ApplicationStatus waitForFinish(long timeoutMs) {
        return ApplicationStatus.Running;
    }

    @Override
    public ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs) {
        return ApplicationStatus.Running;
    }

    @Override
    public ApplicationStatus getStatus() {
        return ApplicationStatus.Running;
    }

    private DMScheduler getDMScheduler(String DMSchedulerClass) {
        DMScheduler scheduler = null;
        try {
            scheduler = (DMScheduler) Class.forName(DMSchedulerClass).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return scheduler;
    }

}
