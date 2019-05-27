package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;

import java.util.*;

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
            completed = new HashMap<>();
        }
    }
    Map<String, PartitionState> partitionStates;
    Map<String, ExecutorState> executorStates;
    List<Long> timePoints;
    public DelayEstimator(){
        partitionStates = new HashMap<>();
        executorStates = new HashMap<>();
        timePoints = new ArrayList<>();
    }
    protected List<Long> getTimePoints(){
        return timePoints;
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
    public void updateExecutorCompleted(String executorId, long time, long completed){
        executorStates.putIfAbsent(executorId, new ExecutorState());
        executorStates.get(executorId).completed.put(time, completed);
    }
    public long getExecutorCompleted(String executorId, long time){
        long completed = 0;
        if(executorStates.containsKey(executorId) && executorStates.get(executorId).completed.containsKey(time)){
            completed = executorStates.get(executorId).completed.get(time);
        }
        return completed;
    }
    public long getPartitionArrived(String partitionId, long time){
        long arrived = 0;
        if(partitionStates.containsKey(partitionId) && partitionStates.get(partitionId).arrived.containsKey(time)){
            arrived = partitionStates.get(partitionId).arrived.get(time);
        }
        return arrived;
    }
    public long getPartitionCompleted(String partitionId, long time){
        long completed = 0;
        if(partitionStates.containsKey(partitionId) && partitionStates.get(partitionId).completed.containsKey(time)){
            completed = partitionStates.get(partitionId).completed.get(time);
        }
        return completed;
    }
    public long getPartitionBacklog(String partitionId, long time, String executorId){
        long backlog = 0;
        if(partitionStates.containsKey(partitionId) && partitionStates.get(partitionId).backlog.containsKey(time)){
            backlog = partitionStates.get(partitionId).backlog.get(time).getOrDefault(executorId, 0l);
        }
        return backlog;
    }
    public void updateAtTime(long time, Map<String, Long> taskArrived, Map<String, Long> taskProcessed, JobModel jobModel) { //Normal update
        timePoints.add(time);
        for (String executorId : jobModel.getContainers().keySet()) {
            ContainerModel containerModel = jobModel.getContainers().get(executorId);
            long d_completed = 0;
            for (TaskName taskName : containerModel.getTasks().keySet()) {
                String id = taskName.getTaskName();
                long arrived = taskArrived.getOrDefault(id, 0l);
                long processed = taskProcessed.getOrDefault(id, 0l);
                updatePartitionArrived(id, time, arrived);
                updatePartitionCompleted(id, time, processed);
                //Update partition backlog
                long backlog = 0;
                if (timePoints.size() > 1) {
                    long lastTime = timePoints.get(timePoints.size() - 2);
                    backlog = getPartitionBacklog(id, lastTime, executorId);
                    backlog -= getPartitionArrived(id, lastTime);
                    backlog += getPartitionCompleted(id, lastTime);
                    d_completed -= getPartitionCompleted(id, lastTime);
                }
                backlog += arrived - processed;
                d_completed += processed;
                updatePartitionBacklog(id, time, executorId, backlog);
            }
            if (timePoints.size() > 1) {
                long lastTime = timePoints.get(timePoints.size() - 2);
                d_completed += getExecutorCompleted(executorId, lastTime);
            }
            updateExecutorCompleted(executorId, time, d_completed);
        }
    }
    public double findArrivedTime(String executorId, long completed){
        long lastTime = 0;
        long lastArrived = 0;
        for(int i=0;i<timePoints.size(); i++){
            long time = timePoints.get(i);
            long arrived = getExecutorCompleted(executorId, time);
            for(String id:partitionStates.keySet()){
                arrived += getPartitionBacklog(id, time, executorId);
            }
            if(arrived >= completed){
                return (arrived - lastArrived) / (double)(time - lastTime);
            }
            lastTime = time;
            lastArrived = arrived;
        }
        return -1;
    }

    public double estimateDelay(String executorId, long time, long lastTime){
        double delay = 0;
        long size = 0;
        long tTime, tLastTime = 0;
        for(int i = 0; i < timePoints.size(); i++){
            tTime = timePoints.get(i);
            if(tTime > time){
                break;
            }
            if(tTime >= lastTime){
                long completed = getExecutorCompleted(executorId, tTime);
                long lastCompleted = getExecutorCompleted(executorId, tLastTime);
                delay += (completed - lastCompleted) * findArrivedTime(executorId, completed);
                size += completed - lastCompleted;
            }
            tLastTime = tTime;
        }
        if(size > 0) delay /= size;
        return delay;
    }

    public void migration(long time, long lastTime){

    }
}
