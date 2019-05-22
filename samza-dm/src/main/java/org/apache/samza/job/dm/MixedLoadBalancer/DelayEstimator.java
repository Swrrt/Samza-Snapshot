package org.apache.samza.job.dm.MixedLoadBalancer;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class DelayEstimator {
    private class PartitionState{
        Map<Long, Long> arrived, completed;
        Map<Long, HashMap<String, Long>> backlog;
    }
    private class ExecutorState{
        Map<Long, Long> completed;
        public ExecutorState(){
        }
    }
    Map<String, PartitionState> partitions;
}
