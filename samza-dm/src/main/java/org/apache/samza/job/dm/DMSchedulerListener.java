package org.apache.samza.job.dm;

import org.apache.samza.config.Config;

public interface DMSchedulerListener {

    /**
     * start the listener
     */
    void startListener();

    void setScheduler(DMScheduler scheduler);

    void setConfig(Config config);
}
