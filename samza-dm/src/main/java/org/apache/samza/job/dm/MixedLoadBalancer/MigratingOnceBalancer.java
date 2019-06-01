package org.apache.samza.job.dm.MixedLoadBalancer;

import javafx.util.Pair;

import java.util.*;

public class MigratingOnceBalancer {
    private ModelingData modelingData;
    public MigratingOnceBalancer(){
    }
    public void setModelingData(ModelingData data){
        modelingData = data;
    }
    private class DFSState{
        String srcContainer, tgtContainer;
        double srcArrivalRate, tgtArrivalRate, srcServiceRate, tgtServiceRate;
        double srcResidual, tgtResidual;
        long time;
        List<String> srcPartitions;
        List<String> tgtPartitions;
        Set<String> migratingPartitions;
        double bestDelay;
        Set<String> bestMigration;
        String bestSrcContainer, bestTgtContainer;
        DFSState(){
            bestDelay = 1e100;
            migratingPartitions = new HashSet<>();
            bestMigration = new HashSet<>();
        }
        protected boolean okToMigratePartition(String partition){
            double partitionArrivalRate = modelingData.getPartitionArriveRate(partition, time);
            return (partitionArrivalRate + tgtArrivalRate < tgtServiceRate - 1e-9);

        }
        protected void migratingPartition(String partition){
            migratingPartitions.add(partition);
            double arrivalRate = modelingData.getPartitionArriveRate(partition, time);
            srcArrivalRate -= arrivalRate;
            tgtArrivalRate += arrivalRate;
        }
        protected void unmigratingPartition(String partition){
            migratingPartitions.remove(partition);
            double arrivalRate = modelingData.getPartitionArriveRate(partition, time);
            srcArrivalRate += arrivalRate;
            tgtArrivalRate -= arrivalRate;
        }
    }
    private double estimateSrcDelay(DFSState state){
        double srcRho = state.srcArrivalRate/state.srcServiceRate;
        double srcDelay = state.srcResidual * srcRho/ (1 - srcRho) + 1/state.srcServiceRate;
        return srcDelay;
    }
    private double estimateTgtDelay(DFSState state) {
        double tgtRho = state.tgtArrivalRate / state.tgtServiceRate;
        double tgtDelay = state.tgtResidual * tgtRho / (1 - tgtRho) + 1 / state.tgtServiceRate;
        return tgtDelay;
    }
    private void DFSforBestDelay(int i, DFSState state){
        double estimateSrc = estimateSrcDelay(state), estimateTgt = estimateTgtDelay(state);

        if(estimateTgt > estimateSrc && estimateSrc > state.bestDelay)return ;

        if(estimateSrc < state.bestDelay && estimateTgt < state.bestDelay){
            state.bestDelay = Math.max(estimateSrc, estimateTgt);
            state.bestMigration.clear();
            state.bestMigration.addAll(state.migratingPartitions);
            state.bestTgtContainer = state.tgtContainer;
            state.bestSrcContainer = state.srcContainer;
        }

        if(i < 0){return ;}

        String partitionId = state.srcPartitions.get(i);

        DFSforBestDelay(i-1, state); //Don't migrate i

        if(state.okToMigratePartition(partitionId)) { //Migrate i
            state.migratingPartition(partitionId);
            DFSforBestDelay(i - 1, state);
            state.unmigratingPartition(partitionId);
        }
    }
    public Map<String, String> rebalance(Map<String, String> oldTaskContainer){
        Map<String, List<String>> containerTasks = new HashMap<>();
        long time = modelingData.getCurrentTime();

        for(String partitionId: oldTaskContainer.keySet()){
            String containerId = oldTaskContainer.get(partitionId);
            if(!containerTasks.containsKey(containerId)){
                containerTasks.put(containerId, new ArrayList<>());
            }
            containerTasks.get(containerId).add(partitionId);
        }
        DFSState dfsState = new DFSState();
        dfsState.time = time;
        for(String srcContainer: containerTasks.keySet()) {
            double srcArrivalRate = modelingData.getExecutorArrivalRate(srcContainer, time);
            double srcServiceRate = modelingData.getExecutorServiceRate(srcContainer, time);
            if (srcArrivalRate < srcServiceRate - 1e-9 ) {
                double srcRho = srcArrivalRate / srcServiceRate;
                for (String tgtContainer : containerTasks.keySet())
                    if (!srcContainer.equals(tgtContainer)) {
                        double tgtArrivalRate = modelingData.getExecutorArrivalRate(tgtContainer, time);
                        double tgtServiceRate = modelingData.getExecutorServiceRate(tgtContainer, time);
                        if (tgtArrivalRate < tgtServiceRate - 1e-9) {
                            double tgtRho = tgtArrivalRate / tgtServiceRate;
                            double otherDelays = 0;
                            for (String otherContainer : containerTasks.keySet()) {
                                if (!otherContainer.equals(srcContainer) && !otherContainer.equals(tgtContainer)) {
                                    double delay = modelingData.getAvgDelay(otherContainer, time);
                                    if (otherDelays < delay)otherDelays = delay;
                                }
                            }
                            if(otherDelays < dfsState.bestDelay - 1e-9){
                                int srcSize = containerTasks.get(srcContainer).size();
                                dfsState.srcPartitions = containerTasks.get(srcContainer);
                                dfsState.tgtPartitions = containerTasks.get(tgtContainer);
                                dfsState.srcArrivalRate = modelingData.getExecutorArrivalRate(srcContainer, time);
                                dfsState.srcServiceRate = modelingData.getExecutorServiceRate(srcContainer, time);
                                dfsState.srcResidual = modelingData.getAvgResidual(srcContainer, time);
                                dfsState.tgtArrivalRate = modelingData.getExecutorArrivalRate(tgtContainer, time);
                                dfsState.tgtServiceRate = modelingData.getExecutorServiceRate(tgtContainer, time);
                                dfsState.tgtResidual = modelingData.getAvgResidual(tgtContainer, time);
                                dfsState.migratingPartitions.clear();
                                dfsState.srcContainer = srcContainer;
                                dfsState.tgtContainer = tgtContainer;
                                DFSforBestDelay(srcSize - 1, dfsState);
                            }
                        }
                    }
            }
        }
        if(dfsState.bestDelay > 1e99){
            writeLog("Cannot find any better migration");
            return oldTaskContainer;
        }
        writeLog("Find best migration with delay: " + dfsState.bestDelay + ", from container " + dfsState.srcContainer + " to container " + dfsState.tgtContainer + ", partitions: " + dfsState.bestMigration);
        Map<String, String> newTaskContainer = new HashMap<>(oldTaskContainer);
        for(String parition: dfsState.bestMigration){
            newTaskContainer.put(parition, dfsState.bestTgtContainer);
        }
        return newTaskContainer;
    }
    private void writeLog(String string){
        System.out.println("MigratingOnceBalancer: " + string);
    }
}
