package org.apache.samza.job.dm.MixedLoadBalancer;

import javafx.util.Pair;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;

import java.awt.*;
import java.util.*;
import java.util.List;

public class ModelingData {
    private class PartitionData{
        Map<Long, Double> arrivalRate;
        PartitionData(){
            arrivalRate = new HashMap<>();
        }
    }
    private class ExecutorData{
        Map<Long, Double> arrivalRate;
        Map<Long, Double> serviceRate;
        Map<Long, Double> avgDelay;
        Map<Long, Double> avgResidual;
        Map<Long, Double> utilization;
        ExecutorData(){
            arrivalRate = new HashMap<>();
            serviceRate = new HashMap<>();
            avgDelay = new HashMap<>();
            avgResidual = new HashMap<>();
            utilization = new HashMap<>();
        }
    }
    private Map<String, ExecutorData> executors;
    private Map<String, PartitionData> partitions;
    private List<Long> times;
    private DelayEstimator delayEstimator;
    private Map<String, Deque<Pair<Long, Double>>> delayWindows;
    private int alpha = 1, beta = 2;
    private long interval = 0;
    public ModelingData(){
        executors = new HashMap<>();
        partitions = new HashMap<>();
        delayWindows = new HashMap<>();
    }
    public void setTimes(long interval, int a, int b){
        this.interval = interval;
        alpha = a;
        beta = b;
    }
    public void setTimes(List<Long> times){
        this.times = times;
    }
    public void setDelayEstimator(DelayEstimator delayEstimator){
        this.delayEstimator = delayEstimator;
    }
    public double getExecutorArrivalRate(String executorId, long time){
        return executors.get(executorId).arrivalRate.getOrDefault(time, 0.0);
    }
    public double getExecutorServiceRate(String executorId, long time) {
        return executors.get(executorId).serviceRate.getOrDefault(time, 0.0);
    }
    public double getAvgDelay(String executorId, long time){
        return executors.get(executorId).avgDelay.getOrDefault(time, 0.0);
    }
    public double getAvgResidual(String executorId, long time){
        return executors.get(executorId).avgResidual.getOrDefault(time, 0.0);
    }
    public double getUtilization(String executorId, long time){
        return executors.get(executorId).utilization.getOrDefault(time, 0.0);
    }
    public double getUtilization(String executorId, long time, long lastTime){
        double sum = 0;
        int numberOfInterval = 0;
        for(int i = times.size() - 1; i>=0; i--){
            long tTime = times.get(i);
            if(tTime < lastTime)break;
            if(tTime <= time){
                numberOfInterval ++;
                sum += getUtilization(executorId, tTime);
            }
        }
        if(numberOfInterval == 0)return 0;
        else return sum/numberOfInterval;
    }
    public void updatePartitionArriveRate(String partitionId, long time, double value){
        if(!partitions.containsKey(partitionId)){
            partitions.put(partitionId, new PartitionData());
        }
        partitions.get(partitionId).arrivalRate.put(time, value);
    }
    public void updateExecutorArriveRate(String executorId, long time, double value){
        if(!executors.containsKey(executorId)){
            executors.put(executorId, new ExecutorData());
        }
        executors.get(executorId).arrivalRate.put(time, value);
    }
    public void updateExecutorServiceRate(String executorId, long time, double value){
        if(!executors.containsKey(executorId)){
            executors.put(executorId, new ExecutorData());
        }
        executors.get(executorId).serviceRate.put(time, value);
    }
    public void updateExecutorUtilization(String executorId, long time, double value){
        if(!executors.containsKey(executorId)){
            executors.put(executorId, new ExecutorData());
        }
        executors.get(executorId).utilization.put(time, value);
    }
    public void updateAvgDelay(String executorId, long time, double value){
        if(!executors.containsKey(executorId)){
            executors.put(executorId, new ExecutorData());
        }
        executors.get(executorId).avgDelay.put(time, value);
    }
    public void updateAvgResidual(String executorId, long time, double value){
        if(!executors.containsKey(executorId)){
            executors.put(executorId, new ExecutorData());
        }
        executors.get(executorId).avgResidual.put(time, value);
    }
    public long getLastTime(long time){
        long lastTime = 0;
        for(int i = times.size() - 1; i>=0;i--){
            lastTime = times.get(i);
            if(lastTime < time)break;
        }
        return lastTime;
    }
    public void updateAtTime(long time, Map<String, Double> containerUtilization, JobModel jobModel){
        for(Map.Entry<String, ContainerModel> entry: jobModel.getContainers().entrySet()) {
            String containerId = entry.getKey();
            double s_arrivalRate = 0;
            long lastTime = getLastTime(time - beta * interval);
            for (TaskName taskName : entry.getValue().getTasks().keySet()) {
                String partitionId = taskName.getTaskName();
                long arrived = delayEstimator.getPartitionArrived(partitionId, time);
                long lastArrived = delayEstimator.getPartitionArrived(partitionId, lastTime);
                double arrivalRate = 0;
                if(time > lastTime) arrivalRate = (arrived - lastArrived) / ((double) time - lastTime);
                updatePartitionArriveRate(partitionId, time, arrivalRate);
                s_arrivalRate += arrivalRate;
            }
            updateExecutorArriveRate(containerId, time, s_arrivalRate);

            //Update actual service rate (capability)
            long completed = delayEstimator.getExecutorCompleted(containerId, time);
            long lastCompleted = delayEstimator.getExecutorCompleted(containerId, lastTime);
            double util = containerUtilization.getOrDefault(containerId, 1.0);
            updateExecutorUtilization(containerId, time, util);
            util = getUtilization(containerId, time, lastTime);
            if(util < 1e-10){
                //TODO: change this
                util = 1;
            }
            double serviceRate = 0;
            if(time > lastTime) serviceRate = (completed - lastCompleted)/(((double)time - lastTime) * util);
            updateExecutorServiceRate(containerId, time, serviceRate);

            //Update avg delay
            double delay = delayEstimator.estimateDelay(containerId, time, time);
            if(!delayWindows.containsKey(containerId)){
                delayWindows.put(containerId, new LinkedList<>());
            }
            Deque<Pair<Long, Double>> window = delayWindows.get(containerId);
            window.addLast(new Pair(time, delay));
            while(window.size() > alpha){
                window.pollFirst();
            }
            Iterator<Pair<Long, Double>> iterator = window.iterator();
            double s_Delay = 0;
            while(iterator.hasNext()){
                s_Delay += iterator.next().getValue();
            }
            double avgDelay = s_Delay / alpha;
            updateAvgDelay(containerId, time, avgDelay);

            //Update residual
            double avgResidual = getAvgResidual(containerId, lastTime);
            double rho = s_arrivalRate / serviceRate;
            if(rho < 1 && rho > 1e-9){
                avgResidual = (avgDelay - 1 / serviceRate) * (1 - rho) / rho;
            }
            updateAvgResidual(containerId, time, avgResidual);
        }
    }
}
