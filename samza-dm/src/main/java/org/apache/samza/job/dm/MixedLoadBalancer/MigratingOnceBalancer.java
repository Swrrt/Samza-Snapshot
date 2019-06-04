package org.apache.samza.job.dm.MixedLoadBalancer;

import javafx.util.Pair;

import java.util.*;

public class MigratingOnceBalancer {
    private ModelingData modelingData;
    private DelayEstimator delayEstimator;
    public MigratingOnceBalancer() {
    }

    public void setModelingData(ModelingData data, DelayEstimator delay) {
        modelingData = data;
        delayEstimator = delay;
    }

    private class DFSState {
        String srcContainer, tgtContainer;
        double srcArrivalRate, tgtArrivalRate, srcServiceRate, tgtServiceRate;
        double srcResidual, tgtResidual;
        long time;
        double otherDelay;
        List<String> srcPartitions;
        List<String> tgtPartitions;
        Set<String> migratingPartitions;
        double bestDelay;
        Set<String> bestMigration;
        String bestSrcContainer, bestTgtContainer;

        DFSState() {
            bestDelay = 1e100;
            migratingPartitions = new HashSet<>();
            bestMigration = new HashSet<>();
        }

        protected boolean okToMigratePartition(String partition) {
            double partitionArrivalRate = modelingData.getPartitionArriveRate(partition, time);
            return (partitionArrivalRate + tgtArrivalRate < tgtServiceRate - 1e-12);

        }

        protected void migratingPartition(String partition) {
            migratingPartitions.add(partition);
            double arrivalRate = modelingData.getPartitionArriveRate(partition, time);
            srcArrivalRate -= arrivalRate;
            tgtArrivalRate += arrivalRate;
        }

        protected void unmigratingPartition(String partition) {
            migratingPartitions.remove(partition);
            double arrivalRate = modelingData.getPartitionArriveRate(partition, time);
            srcArrivalRate += arrivalRate;
            tgtArrivalRate -= arrivalRate;
        }
    }

    private double estimateSrcDelay(DFSState state) {
        double srcRho = state.srcArrivalRate / state.srcServiceRate;
        double srcDelay = state.srcResidual * srcRho / (1 - srcRho) + 1 / state.srcServiceRate;
        return srcDelay;
    }

    private double estimateTgtDelay(DFSState state) {
        double tgtRho = state.tgtArrivalRate / state.tgtServiceRate;
        double tgtDelay = state.tgtResidual * tgtRho / (1 - tgtRho) + 1 / state.tgtServiceRate;
        return tgtDelay;
    }

    private void Bruteforce(int i, DFSState state){
        for(long migrated = (1<<i) - 1; migrated > 0; migrated --){
            double srcArrivalRate = state.srcArrivalRate;
            double tgtArrivalRate = state.tgtArrivalRate;
            state.migratingPartitions.clear();
            for(int  j = 0; j < i; j++)
                if(((1<<j) & migrated) > 0){
                    String partitionId = state.srcPartitions.get(j);
                    state.migratingPartitions.add(partitionId);
                    double partitionArrivalRate = modelingData.getPartitionArriveRate(partitionId, state.time);
                    srcArrivalRate -= partitionArrivalRate;
                    tgtArrivalRate += partitionArrivalRate;
                }
            if(srcArrivalRate < state.srcServiceRate && tgtArrivalRate < state.tgtServiceRate){
                double srcRho = srcArrivalRate / state.srcServiceRate;
                double srcDelay = state.srcResidual * srcRho / (1 - srcRho) + 1 / state.srcServiceRate;
                double tgtRho = tgtArrivalRate / state.tgtServiceRate;
                double tgtDelay = state.tgtResidual * tgtRho / (1 - tgtRho) + 1 / state.tgtServiceRate;
                if(srcDelay < state.bestDelay && tgtDelay < state.bestDelay && state.otherDelay < state.bestDelay){
                    state.bestDelay = Math.max(Math.max(srcDelay, tgtDelay), state.otherDelay);
                    state.bestTgtContainer = state.tgtContainer;
                    state.bestSrcContainer = state.srcContainer;
                    state.bestMigration.clear();
                    state.bestMigration.addAll(state.migratingPartitions);
                }
            }
        }
    }

    private void DFSforBestDelay(int i, DFSState state) {
        if (state.otherDelay > state.bestDelay - 1e-9) return;
        if(state.srcArrivalRate < state.srcServiceRate && state.tgtArrivalRate < state.tgtServiceRate) {
            double estimateSrc = estimateSrcDelay(state), estimateTgt = estimateTgtDelay(state);
            writeLog("If migrating partitions " + state.migratingPartitions
                    + " from " +state.srcContainer
                    + " to " + state.tgtContainer
                    + ", estimate source delay: " + estimateSrc
                    + ", estimate target delay: " + estimateTgt
                    + ", current best delay: " + state.bestDelay
                    + ", srcArrivalRate: " + state.srcArrivalRate
                    + ", tgtArrivalRate: " + state.tgtArrivalRate
                    + ", srcServiceRate: " + state.srcServiceRate
                    + ", tgtServiceRate: " + state.tgtServiceRate
                    + ", srcResidual: " + state.srcResidual
                    + ", tgtResidual: " +state.tgtResidual
            );
            if (estimateTgt > estimateSrc && estimateSrc > state.bestDelay) return;
            if (estimateSrc < state.bestDelay && estimateTgt < state.bestDelay) {
                state.bestDelay = Math.max(Math.max(estimateSrc, estimateTgt), state.otherDelay);
                state.bestMigration.clear();
                state.bestMigration.addAll(state.migratingPartitions);
                state.bestTgtContainer = state.tgtContainer;
                state.bestSrcContainer = state.srcContainer;
            }
        }
        if (i < 0) {
            return;
        }

        //String partitionId = state.srcPartitions.get(i);

        for(int j = i - 1; j >= 0; j--) {
            String partitionId = state.srcPartitions.get(j);
            if (state.okToMigratePartition(partitionId)) { //Migrate j
                state.migratingPartition(partitionId);
                DFSforBestDelay(j, state);
                state.unmigratingPartition(partitionId);
            }
        }
    }

    public Map<String, String> rebalance(Map<String, String> oldTaskContainer, double threshold) {
        writeLog("Migrating once based on tasks: " + oldTaskContainer);
        Map<String, List<String>> containerTasks = new HashMap<>();
        long time = modelingData.getCurrentTime();
        for (String partitionId : oldTaskContainer.keySet()) {
            String containerId = oldTaskContainer.get(partitionId);
            if (!containerTasks.containsKey(containerId)) {
                containerTasks.put(containerId, new ArrayList<>());
            }
            containerTasks.get(containerId).add(partitionId);
        }
        if(containerTasks.keySet().size() == 0 ){
            return oldTaskContainer;
        }
        DFSState dfsState = new DFSState();
        dfsState.time = time;

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
        if(srcContainer.equals("")){
            writeLog("Cannot find container that exceeds threshold");
            return oldTaskContainer;
        }
        if(containerTasks.get(srcContainer).size() <= 1){
            writeLog("Largest delay container " + srcContainer + " has only " + containerTasks.get(srcContainer).size());
            return oldTaskContainer;
        }
        dfsState.bestDelay = initialDelay;
        dfsState.bestSrcContainer = srcContainer;
        dfsState.bestTgtContainer =  srcContainer;
        dfsState.bestMigration.clear();
        //Migrating this container
        dfsState.srcContainer = srcContainer;
        dfsState.srcArrivalRate = modelingData.getExecutorArrivalRate(srcContainer, time);
        dfsState.srcServiceRate = modelingData.getExecutorServiceRate(srcContainer, time);
        dfsState.srcResidual = modelingData.getAvgResidual(srcContainer, time);
        dfsState.srcPartitions = containerTasks.get(srcContainer);

        for (String tgtContainer : containerTasks.keySet())
            if (!srcContainer.equals(tgtContainer)) {
                double tgtArrivalRate = modelingData.getExecutorArrivalRate(tgtContainer, time);
                double tgtServiceRate = modelingData.getExecutorServiceRate(tgtContainer, time);
                if (tgtArrivalRate < tgtServiceRate - 1e-9) {
                    double otherDelays = 0;
                    for (String otherContainer : containerTasks.keySet()) {
                        if (!otherContainer.equals(srcContainer) && !otherContainer.equals(tgtContainer)) {
                            double delay = modelingData.getAvgDelay(otherContainer, time);
                            if (otherDelays < delay) otherDelays = delay;
                        }
                    }
                    if (otherDelays < dfsState.bestDelay - 1e-9) {
                        int srcSize = containerTasks.get(srcContainer).size();
                        dfsState.otherDelay = otherDelays;
                        dfsState.tgtPartitions = containerTasks.get(tgtContainer);
                        dfsState.tgtArrivalRate = tgtArrivalRate;
                        dfsState.tgtServiceRate = tgtServiceRate;
                        dfsState.tgtResidual = modelingData.getAvgResidual(tgtContainer, time);
                        dfsState.migratingPartitions.clear();
                        dfsState.tgtContainer = tgtContainer;
                        //DFSforBestDelay(srcSize, dfsState);
                        Bruteforce(srcSize, dfsState);
                    }
                }
            }

        if (dfsState.bestDelay > initialDelay - 1e-9 || dfsState.bestDelay > threshold) {
            writeLog("Cannot find any better migration");
            return oldTaskContainer;
        }

        writeLog("Find best migration with delay: " + dfsState.bestDelay + ", from container " + dfsState.bestSrcContainer + " to container " + dfsState.bestTgtContainer + ", partitions: " + dfsState.bestMigration);

        Map<String, String> newTaskContainer = new HashMap<>();
        newTaskContainer.putAll(oldTaskContainer);
        for (String parition : dfsState.bestMigration) {
            delayEstimator.migration(time, srcContainer, dfsState.bestTgtContainer, parition);
            newTaskContainer.put(parition, dfsState.bestTgtContainer);
        }
        return newTaskContainer;
    }

    private void writeLog(String string) {
        System.out.println("MigratingOnceBalancer: " + string);
    }
}
