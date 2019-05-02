package org.apache.samza.job.dm;

import java.util.logging.Logger;

public class DefaultSchedulingPolicy implements DMSchedulingPolicy {

    private static final Logger LOG = Logger.getLogger(DefaultScheduler.class.getName());

    @Override
    public Allocation allocate(Stage curr, StageReport report) {
        if (report.getThroughput() > 160 && curr.getRunningContainers() == 1) {
            LOG.info("Requesting scaling of containers");
            return new Allocation(report.getName(), 1);
        }
        return new Allocation(report.getName(), 0);
    }
}
