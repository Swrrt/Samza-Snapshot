package org.apache.samza.job.dm.StreamSwitch;


import java.util.Map;
import java.util.Set;

//Direct read metrics from containers
public interface MetricsRetriever {
    void updateContainerIds(Set<String> containerIds);
    void retrieveMetrics();
    Map<String, Long> getTaskProcessed();
    Map<String, Long> getTaskArrived();
    Map<String, Long> getContainerProcessed();
    Map<String, Long> getContainerArrived();
    Map<String, Long> getContainerJobModelVersion();
    Map<String, Double> getContainerUtilization();
}
