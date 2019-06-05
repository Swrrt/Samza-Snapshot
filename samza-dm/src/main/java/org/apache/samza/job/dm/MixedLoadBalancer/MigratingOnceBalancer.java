package org.apache.samza.job.dm.MixedLoadBalancer;

import javafx.util.Pair;
import org.apache.samza.job.dm.MixedLoadBalanceDM.RebalanceResult;

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

    public double estimateDelay(double arrivalRate, double serviceRate, double residual) {
        double rho = arrivalRate / serviceRate;
        return rho / (1 - rho) * residual + 1 / serviceRate;
    }

    private double estimateSrcDelay(DFSState state) {
        return estimateDelay(state.srcArrivalRate, state.srcServiceRate, state.srcResidual);
    }

    private double estimateTgtDelay(DFSState state) {
        return estimateDelay(state.tgtArrivalRate, state.tgtServiceRate, state.tgtResidual);
    }

    private void Bruteforce(int i, DFSState state) {
        for (long migrated = (1 << i) - 1; migrated > 0; migrated--) {
            double srcArrivalRate = state.srcArrivalRate;
            double tgtArrivalRate = state.tgtArrivalRate;
            state.migratingPartitions.clear();
            for (int j = 0; j < i; j++)
                if (((1 << j) & migrated) > 0) {
                    String partitionId = state.srcPartitions.get(j);
                    state.migratingPartitions.add(partitionId);
                    double partitionArrivalRate = modelingData.getPartitionArriveRate(partitionId, state.time);
                    srcArrivalRate -= partitionArrivalRate;
                    tgtArrivalRate += partitionArrivalRate;
                }
            if (srcArrivalRate < state.srcServiceRate && tgtArrivalRate < state.tgtServiceRate) {
                double srcRho = srcArrivalRate / state.srcServiceRate;
                double srcDelay = state.srcResidual * srcRho / (1 - srcRho) + 1 / state.srcServiceRate;
                double tgtRho = tgtArrivalRate / state.tgtServiceRate;
                double tgtDelay = state.tgtResidual * tgtRho / (1 - tgtRho) + 1 / state.tgtServiceRate;
                writeLog("Migrating " + state.migratingPartitions
                        + " to " + state.tgtContainer
                        + ", srcArrival: " + srcArrivalRate
                        + ", srcDelay: " + srcDelay
                        + ", tgtArrival: " + tgtArrivalRate
                        + ", tgtDelay: " + tgtDelay
                        + ", bestDelay: " + state.bestDelay
                );
                if (srcDelay < state.bestDelay && tgtDelay < state.bestDelay) {
                    state.bestDelay = Math.max(srcDelay, tgtDelay);
                    state.bestTgtContainer = state.tgtContainer;
                    state.bestSrcContainer = state.srcContainer;
                    state.bestMigration.clear();
                    state.bestMigration.addAll(state.migratingPartitions);
                }
            }
        }
        writeLog("From container " + state.srcContainer
                + " to container " + state.tgtContainer
                + " , best migration delay: " + state.bestDelay
                + " , best migration: " + state.bestMigration
        );
    }

    private void DFSforBestDelay(int i, DFSState state) {
        if (state.srcArrivalRate < state.srcServiceRate && state.tgtArrivalRate < state.tgtServiceRate) {
            double estimateSrc = estimateSrcDelay(state), estimateTgt = estimateTgtDelay(state);
            writeLog("If migrating partitions " + state.migratingPartitions
                    + " from " + state.srcContainer
                    + " to " + state.tgtContainer
                    + ", estimate source delay: " + estimateSrc
                    + ", estimate target delay: " + estimateTgt
                    + ", current best delay: " + state.bestDelay
                    + ", srcArrivalRate: " + state.srcArrivalRate
                    + ", tgtArrivalRate: " + state.tgtArrivalRate
                    + ", srcServiceRate: " + state.srcServiceRate
                    + ", tgtServiceRate: " + state.tgtServiceRate
                    + ", srcResidual: " + state.srcResidual
                    + ", tgtResidual: " + state.tgtResidual
            );
            if (estimateTgt > estimateSrc && estimateSrc > state.bestDelay) return;
            if (estimateSrc < state.bestDelay && estimateTgt < state.bestDelay) {
                state.bestDelay = Math.max(estimateSrc, estimateTgt);
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

        for (int j = i - 1; j >= 0; j--) {
            String partitionId = state.srcPartitions.get(j);
            if (state.okToMigratePartition(partitionId)) { //Migrate j
                state.migratingPartition(partitionId);
                DFSforBestDelay(j, state);
                state.unmigratingPartition(partitionId);
            }
        }
    }

    public RebalanceResult rebalance(Map<String, String> oldTaskContainer, double threshold) {
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
        if (containerTasks.keySet().size() == 0) { //No container to move
            RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.Unable, oldTaskContainer);
            return result;
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
        writeLog("Try to migrate from largest delay container " + srcContainer);
        dfsState.bestDelay = initialDelay;
        dfsState.bestSrcContainer = srcContainer;
        dfsState.bestTgtContainer = srcContainer;
        dfsState.bestMigration.clear();
        //Migrating this container
        dfsState.srcContainer = srcContainer;
        dfsState.srcArrivalRate = modelingData.getExecutorArrivalRate(srcContainer, time);
        dfsState.srcServiceRate = modelingData.getExecutorServiceRate(srcContainer, time);
        dfsState.srcResidual = modelingData.getAvgResidual(srcContainer, time);
        dfsState.srcPartitions = containerTasks.get(srcContainer);

        //Choose target container based on ideal delay (minimize ideal delay)
        double minIdealDelay = 1e100;
        String tgtContainer = "";
        for (String container : containerTasks.keySet()) {
            if(container.equals(srcContainer)) continue;
            double R1 = dfsState.srcResidual;
            double R2 = modelingData.getAvgResidual(container, time);
            double n1 = dfsState.srcArrivalRate;
            double n2 = modelingData.getExecutorArrivalRate(container, time);
            double u1 = dfsState.srcServiceRate;
            double u2 = modelingData.getExecutorServiceRate(container, time);
            if (n1 + n2 < u1 + u2) {
                // A = ((R2 - R1) * u1 * u2 + (u2 - u1))
                double A = (R2 - R1) * u1 * u2 + (u2 - u1);
                //Transform to Ax^2 + Bx + c = 0
                // B = A(n1 - n2) + u1 * u2 * (R1 * u2 + R2 * u1) - (u2 - u1)^2
                double B = A * (n2 - n1)
                        + u1 * u2 * (R1 * u2 + R2 *u1)
                        - (u2 - u1) * (u2 - u1);
                // C = -(A * n1 * n2 + ((R1 * n1 + 1) * u1 * u2 + (u1 - u2) * n1) * u2 - ((R2 * n2 + 1) * u2 * u1 + (u2 - u1) * n2) * u1)
                double C = -(
                        A * n1 * n2
                        + ((R1 * n1 + 1) * u1 * u2 + (u1 - u2) * n1) * u2
                        - ((R2 * n2 + 1) * u2 * u1 + (u2 - u1) * n2) * u1
                        );
                //Solve x
                double delta = B * B - 4 * A * C;
                if (delta > -1e-16) {
                    double rDelta = Math.sqrt(delta + 1e-16);
                    double x1 = (-B + rDelta) / (2 * A);
                    double x2 = (-B - rDelta) / (2 * A);
                    writeLog("A: " + A
                            + ", B: " + B
                            + ", C: " + C
                            + ", x1: " + x1
                            + ", x2: " + x2
                    );
                    writeLog("n1: " + n1
                            + ", u1: " + u1
                            + ", R1: " + R1
                            + ", n2: " + n2
                            + ", u2: " + u2
                            + ", R2: " + R2
                    );
                    if (x1 < 0 || n1 - x1 > u1 || n1 - x1 < 0 || n2 + x1 > u2 || n2 + x1 < 0) {
                        double t = x1;
                        x1 = x2;
                        x2 = t;
                    }
                    if (x1 < 0 || n1 - x1 > u1 || n1 - x1 < 0 || n2 + x1 > u2 || n2 + x1 < 0) {
                        writeLog("Something wrong with ideal delay for " + dfsState.srcContainer + " to " + container);
                    } else {
                        //Debug
                        double d1 = estimateDelay(n1 - x1, u1, R1);
                        double d2 = estimateDelay(n2 + x1, u2, R2);
                        writeLog("Estimate delays for container " + container + " : " + d1 + " , " + d2);
                        if (d1 < minIdealDelay) {
                            minIdealDelay = d1;
                            tgtContainer = container;
                        }
                    }
                }
            }
        }
        if(tgtContainer.equals("")){
            writeLog("Cannot find available migration");
            RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.NeedScalingOut, oldTaskContainer);
            return result;
        }
        writeLog("Find minimal ideal container " + tgtContainer + " , ideal delay: " + minIdealDelay);
/*        for (String tgtContainer : containerTasks.keySet())
            if (!srcContainer.equals(tgtContainer)) {*/
        double tgtArrivalRate = modelingData.getExecutorArrivalRate(tgtContainer, time);
        double tgtServiceRate = modelingData.getExecutorServiceRate(tgtContainer, time);
        if (tgtArrivalRate < tgtServiceRate - 1e-9) {
            int srcSize = containerTasks.get(srcContainer).size();
            dfsState.tgtPartitions = containerTasks.get(tgtContainer);
            dfsState.tgtArrivalRate = tgtArrivalRate;
            dfsState.tgtServiceRate = tgtServiceRate;
            dfsState.tgtResidual = modelingData.getAvgResidual(tgtContainer, time);
            dfsState.migratingPartitions.clear();
            dfsState.tgtContainer = tgtContainer;
            DFSforBestDelay(srcSize, dfsState);
            //Bruteforce(srcSize, dfsState);
        }
/*            } */

        if (dfsState.bestDelay > initialDelay - 1e-9) {
            writeLog("Cannot find any better migration");
            RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.NeedScalingOut, oldTaskContainer);
            return result;
        }

        writeLog("Find best migration with delay: " + dfsState.bestDelay + ", from container " + dfsState.bestSrcContainer + " to container " + dfsState.bestTgtContainer + ", partitions: " + dfsState.bestMigration);

        Map<String, String> newTaskContainer = new HashMap<>();
        newTaskContainer.putAll(oldTaskContainer);
        for (String parition : dfsState.bestMigration) {
            delayEstimator.migration(time, srcContainer, dfsState.bestTgtContainer, parition);
            newTaskContainer.put(parition, dfsState.bestTgtContainer);
        }
        RebalanceResult result = new RebalanceResult(RebalanceResult.RebalanceResultCode.Migrating, newTaskContainer);
        return result;
    }

    private void writeLog(String string) {
        System.out.println("MigratingOnceBalancer: " + string);
    }
}
