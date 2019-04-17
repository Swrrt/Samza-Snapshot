package org.apache.samza.job.dm;

import org.apache.samza.job.ApplicationStatus;

public class Stage {
    private String name;
    private int id;
    private int runningContainers;

    private ApplicationStatus status;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ApplicationStatus getStatus() {
        return status;
    }

    public void setStatus(ApplicationStatus status) {
        this.status = status;
    }

    public void bulkUpdate(String[] data){
        // TODO: split data and update relevant field
    }

    public int getRunningContainers() {
        return runningContainers;
    }

    public void setRunningContainers(int runningContainers) {
        this.runningContainers = runningContainers;
    }

    // TODO: add properties for input/output stream
}
