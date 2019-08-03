package org.apache.samza.job.dm.StreamSwitch;

import java.util.HashMap;
import java.util.Map;

public class MetricsMetadata {
    Map<String, Long> taskProcessed;
    Map<String, Long> taskArrived;
    Map<String, Long> containerProcessed;
    Map<String, Long> containerArrived;
    Map<String, Double> containerUtilization;
    Map<String, Long> containerJobModelVersion;
    boolean isMigration = false;
    public MetricsMetadata(){
        taskProcessed = new HashMap<>();
        taskArrived = new HashMap<>();
        containerArrived = new HashMap<>();
        containerProcessed = new HashMap<>();
        containerUtilization = new HashMap<>();
        containerJobModelVersion = new HashMap<>();
        isMigration = false;
    }
    public MetricsMetadata(Map<String, Long> taskProcessed, Map<String, Long> taskArrived, Map<String, Long> containerProcessed, Map<String, Long> containerArrived, Map<String, Double> containerUtilization, Map<String, Long> containerJobModelVersion){
        this.taskProcessed = taskProcessed;
        this.taskArrived = taskArrived;
        this.containerProcessed = containerProcessed;
        this.containerArrived = containerArrived;
        this.containerUtilization = containerUtilization;
        this.containerJobModelVersion = containerJobModelVersion;
    }
    public MetricsMetadata(Map<String, Long> taskProcessed, Map<String, Long> taskArrived, Map<String, Long> containerProcessed, Map<String, Long> containerArrived, Map<String, Double> containerUtilization,Map<String, Long> containerJobModelVersion, boolean isMigration){
        this.taskProcessed = taskProcessed;
        this.taskArrived = taskArrived;
        this.containerProcessed = containerProcessed;
        this.containerArrived = containerArrived;
        this.containerUtilization = containerUtilization;
        this.containerJobModelVersion = containerJobModelVersion;
        this.isMigration = isMigration;
    }
    public Map<String, Long> getTaskProcessed(){
        return taskProcessed;
    }
    public Map<String, Long> getTaskArrived(){
        return taskArrived;
    }
    public Map<String, Long> getContainerProcessed(){
        return containerProcessed;
    }
    public Map<String, Long> getContainerArrived(){
        return containerArrived;
    }
    public Map<String, Double> getContainerUtilization(){
        return containerUtilization;
    }
    public Map<String, Long> getContainerJobModelVersion(){
        return containerJobModelVersion;
    }
    public void setMigration(boolean isMigration){
        this.isMigration = isMigration;
    }
    public boolean checkMigration(){
        return isMigration;
    }
}
