package org.apache.samza.job.dm;

import org.apache.hadoop.yarn.api.records.Resource;

public class Allocation {

    public Allocation(String stageID) {
        this.stageID = stageID;
        this.parallelism = 1;
    }

    public Allocation(String stageID, int parallelism) {
        this.stageID = stageID;
        this.parallelism = parallelism;
    }

    public Allocation(String stageID, Resource clusterResource, int parallelism) {
        this.stageID = stageID;
        this.clusterResource = clusterResource;
        this.parallelism = parallelism;
    }

    private String stageID;

    private Resource clusterResource;

    private int parallelism;

    public String getStageID() {
        return stageID;
    }

    public void setStageID(String stageID) {
        this.stageID = stageID;
    }

    public Resource getClusterResource() {
        return clusterResource;
    }

    public void setClusterResource(Resource clusterResource) {
        this.clusterResource = clusterResource;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}
