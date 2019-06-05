package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.samza.job.dm.MixedLoadBalanceDM.RebalanceResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrateLargestByNumberScaler {
    private ModelingData modelingData;
    private DelayEstimator delayEstimator;
    public MigrateLargestByNumberScaler(){
    }
    public void setModelingData(ModelingData modelingData, DelayEstimator delayEstimator){
        this.modelingData = modelingData;
        this.delayEstimator = delayEstimator;
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
        double initialDelay = -1.0;
        String srcContainer = "";
        for (String containerId : containerTasks.keySet()) {
            double delay = modelingData.getAvgDelay(containerId, time);
            if (delay > initialDelay) {
                initialDelay = delay;
                srcContainer = containerId;
            }
        }

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
        if(size < numberToScaleOut + 1){
            numberToScaleOut = size - 1;
            writeLog("Does not have enough partitions to divide, now only scale out " + numberToScaleOut + " containers");
        }

        HashMap<String, String> newTaskContainer = new HashMap<>();
        newTaskContainer.putAll(oldTaskContainer);
        int averagePartitions = containerTasks.get(srcContainer).size() / (numberToScaleOut + 1);
        int numbersOfRemainder = containerTasks.get(srcContainer).size() % (numberToScaleOut + 1);
        int currentIndex = 0;
        for(int i = 0; i < numberToScaleOut; i++){
            String containerId = String.format("%06d", containerTasks.size() + 2); //Id start from 000002
            int numberToMove = averagePartitions;
            if(i < numbersOfRemainder) numberToMove ++;
            while(numberToMove > 0){
                String taskId = containerTasks.get(srcContainer).get(currentIndex);
                delayEstimator.migration(time, srcContainer, containerId, taskId);
                newTaskContainer.put(taskId, containerId);
                numberToMove --;
                currentIndex ++;
            }
        }
        writeLog("New task-container mapping: " + newTaskContainer);
        return new RebalanceResult(RebalanceResult.RebalanceResultCode.ScalingOut, newTaskContainer);
    }
    private void writeLog(String string) {
        System.out.println("MigrateLargestByNumberScaler: " + string);
    }
}
