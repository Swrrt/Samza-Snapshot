package org.apache.samza.job.dm.MixedLoadBalancer;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class DelayEstimator {
    private class PartitionState{
        Map<Long, Long> arrived, completed;
        Map<Long, HashMap<String, Long>> backlog;
        PartitionState(){
            arrived = new HashMap<>();
            completed = new HashMap<>();
            backlog = new HashMap<>();
        }
    }
    private class ExecutorState{
        Map<Long, Long> completed;
        public ExecutorState(){
        }
    }
    Map<String, PartitionState> partitionStates;
    Map<String, ExecutorState> executorStates;
    public DelayEstimator(){
        partitionStates = new HashMap<>();
        executorStates = new HashMap<>();
    }
    public void updatePartitionArrived(String partitionId, long time, long arrived){
        partitionStates.putIfAbsent(partitionId, new PartitionState());
        partitionStates.get(partitionId).arrived.put(time, arrived);
    }
    public void updatePartitionCompleted(String partitionId, long time, long completed){
        partitionStates.putIfAbsent(partitionId, new PartitionState());
        partitionStates.get(partitionId).completed.put(time, completed);
    }
    public void updatePartitionBacklog(String partitionId, long time, String executorId, long backlog){
        partitionStates.putIfAbsent(partitionId, new PartitionState());
        partitionStates.get(partitionId).backlog.putIfAbsent(time, new HashMap<>());
        partitionStates.get(partitionId).backlog.get(time).put(executorId, backlog);
    }

}
