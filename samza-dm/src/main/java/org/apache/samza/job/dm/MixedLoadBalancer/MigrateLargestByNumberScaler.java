package org.apache.samza.job.dm.MixedLoadBalancer;

import javafx.util.Pair;
import org.apache.samza.job.dm.MixedLoadBalanceDM.RebalanceResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrateLargestByNumberScaler {
    private ModelingData modelingData;
    private DelayEstimator delayEstimator;
    private MixedLoadBalanceManager loadBalanceManager;
    public MigrateLargestByNumberScaler(){
    }
    public void setModelingData(ModelingData modelingData, DelayEstimator delayEstimator, MixedLoadBalanceManager loadBalanceManager){
        this.modelingData = modelingData;
        this.delayEstimator = delayEstimator;
        this.loadBalanceManager = loadBalanceManager;
    }

    private Pair<String, Double> findMaxInstantDelay(Map<String, List<String>> containerTasks, long time){
        double initialDelay = -1.0;
        String maxContainer = "";
        for (String containerId : containerTasks.keySet()) {
            double instantDelay = modelingData.getAvgDelay(containerId, time);
            if (instantDelay > initialDelay && !loadBalanceManager.checkDelay(containerId)) {
                initialDelay = instantDelay;
                maxContainer = containerId;
            }
        }
        return new Pair(maxContainer, initialDelay);
    }

    private Pair<String, Double> findMaxLongtermDelay(Map<String, List<String>> containerTasks, long time){
        double initialDelay = -1.0;
        String maxContainer = "";
        for (String containerId : containerTasks.keySet()) {
            double longtermDelay = modelingData.getLongTermDelay(containerId, time);
            if (longtermDelay > initialDelay && !loadBalanceManager.checkDelay(containerId)) {
                initialDelay = longtermDelay;
                maxContainer = containerId;
            }
        }
        return new Pair(maxContainer, initialDelay);
    }

    public RebalanceResult scaleInByOne(Map<String, String> oldTaskContainer, double instantaneousThreshold, double longtermThreshold){
        Map<String, List<String>> containerTasks = new HashMap<>();
        writeLog("Try to scale in");
        long time = modelingData.getCurrentTime();
        for (String partitionId : oldTaskContainer.keySet()) {
            String containerId = oldTaskContainer.get(partitionId);
            if (!containerTasks.containsKey(containerId)) {
                containerTasks.put(containerId, new ArrayList<>());
            }
            containerTasks.get(containerId).add(partitionId);
        }
        if (containerTasks.keySet().size() <= 1) { //No container to move
            writeLog("Has one or less containers, cannot scale in");
            RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.Unable, oldTaskContainer);
            return result;
        }

        //Enumerate through all pairs of containers
        for(String srcContainer: containerTasks.keySet()) {
            double srcArrival = modelingData.getExecutorArrivalRate(srcContainer, time);
            for (String tgtContainer : containerTasks.keySet())
                if (!srcContainer.equals(tgtContainer)) {
                    double tgtArrival = modelingData.getExecutorArrivalRate(tgtContainer, time);
                    double tgtService = modelingData.getExecutorServiceRate(tgtContainer, time);
                    double tgtInstantDelay = modelingData.getAvgDelay(tgtContainer, time);
                    if(tgtInstantDelay < instantaneousThreshold && srcArrival + tgtArrival < tgtService){
                        double estimatedLongtermDelay = MigratingOnceBalancer.estimateLongtermDelay(srcArrival + tgtArrival, tgtService);
                        //Scale In
                        if(estimatedLongtermDelay < longtermThreshold){
                            HashMap<String, String> newTaskContainer = new HashMap<>();
                            newTaskContainer.putAll(oldTaskContainer);
                            Map<String, String> migratingTask = new HashMap<>();
                            for(String task: containerTasks.get(srcContainer)){
                                newTaskContainer.put(task, tgtContainer);
                                migratingTask.put(task, tgtContainer);
                            }
                            writeLog("Scale in! from " + srcContainer + " to " + tgtContainer);
                            writeLog("New task-container mapping: " + newTaskContainer);
                            MigrationContext migrationContext = new MigrationContext(srcContainer, "", migratingTask);
                            return new RebalanceResult(RebalanceResult.RebalanceResultCode.ScalingIn, newTaskContainer, migrationContext);
                        }
                    }
                }
        }
        writeLog("Cannot scale in");
        RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.Unable, oldTaskContainer);
        return result;
    }

    public RebalanceResult scaleOutByNumber(Map<String, String> oldTaskContainer, int numberToScaleOut){
        Map<String, List<String>> containerTasks = new HashMap<>();
        writeLog("Scale out " + numberToScaleOut + " containers");
        long time = modelingData.getCurrentTime();
        for (String partitionId : oldTaskContainer.keySet()) {
            String containerId = oldTaskContainer.get(partitionId);
            if (!containerTasks.containsKey(containerId)) {
                containerTasks.put(containerId, new ArrayList<>());
            }
            containerTasks.get(containerId).add(partitionId);
        }
        if (containerTasks.keySet().size() == 0) { //No container to move
            RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.Unable, oldTaskContainer);
            return result;
        }
        //Find container with maximum delay

        Pair<String, Double> a = findMaxLongtermDelay(containerTasks, time);
        String srcContainer = a.getKey();
        double initialDelay = a.getValue();

        if (srcContainer.equals("")) { //No correct container
            writeLog("Cannot find the container that exceeds threshold");
            RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.Unable, oldTaskContainer);
            return result;
        }
        if (containerTasks.get(srcContainer).size() <= 1) { //Container has only one partition
            writeLog("Largest delay container " + srcContainer + " has only " + containerTasks.get(srcContainer).size());
            RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.Unable, oldTaskContainer);
            return result;
        }

        writeLog("Try to scale out and migrate from largest delay container " + srcContainer);
        int size = containerTasks.get(srcContainer).size();
        if(numberToScaleOut > size - 1){
            numberToScaleOut = size - 1;
            writeLog("Does not have enough partitions to divide, now only scale out " + numberToScaleOut + " containers");
        }

        HashMap<String, String> newTaskContainer = new HashMap<>();
        newTaskContainer.putAll(oldTaskContainer);
        int averagePartitions = containerTasks.get(srcContainer).size() / (numberToScaleOut + 1);
        int numbersOfRemainder = containerTasks.get(srcContainer).size() % (numberToScaleOut + 1);
        int currentIndex = 0;
        Map<String, String> migratingTask = new HashMap<>();
        int tgtId = loadBalanceManager.getNextContainerId();
        for(int i = 0; i < numberToScaleOut; i++){
            String containerId = String.format("%06d", tgtId); //Id start from 000002
            int numberToMove = averagePartitions;
            if(i < numbersOfRemainder) numberToMove ++;
            while(numberToMove > 0){
                String taskId = containerTasks.get(srcContainer).get(currentIndex);
                //delayEstimator.migration(time, srcContainer, containerId, taskId);
                migratingTask.put(taskId, containerId);
                newTaskContainer.put(taskId, containerId);
                numberToMove --;
                currentIndex ++;
            }
        }
        writeLog("Scale out and migrate from container " + srcContainer);
        writeLog("New task-container mapping: " + newTaskContainer);
        MigrationContext migrationContext = new MigrationContext(srcContainer, "", migratingTask);
        return new RebalanceResult(RebalanceResult.RebalanceResultCode.ScalingOut, newTaskContainer, migrationContext);
    }
    private void writeLog(String string) {
        System.out.println("MigrateLargestByNumberScaler: " + string);
    }
}
