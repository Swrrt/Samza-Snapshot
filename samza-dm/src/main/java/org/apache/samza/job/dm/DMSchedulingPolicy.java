package org.apache.samza.job.dm;

public interface DMSchedulingPolicy {

    /**
     * Given current stage, assign new Allocation if necessary
     *
     * @return Allocation
     */
    Allocation allocate(Stage curr, StageReport report);
}
