package org.apache.samza.scheduler;

import org.apache.samza.config.Config;

public interface LoadSchedulerRunloop {
    void start();
    void setConfig(Config config);
    void setScheduler(LoadScheduler scheduler);
}
