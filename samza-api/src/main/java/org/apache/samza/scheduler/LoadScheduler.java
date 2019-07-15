package org.apache.samza.scheduler;

import org.apache.samza.config.Config;

/**
 *  Load balance scheduler
 */
public interface LoadScheduler {
    /**
     * Initiate scheduler
     * @param config
     */
    void init(Config config);

    /**
     * start the scheduler
     */
    void start();
}
