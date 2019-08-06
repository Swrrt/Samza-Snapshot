package org.apache.samza.metrics;

import org.apache.samza.dispatcher.LeaderDispatcher;
import org.apache.samza.util.RMI.MetricsClient;
import org.apache.samza.util.RMI.MetricsRetrieverRMIServer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RMIMetricsRetriever implements MetricsRetriever{
    MetricsRetrieverRMIServer rmiServer;
    Map<String, Long> taskProcessed;
    Map<String, Long> taskArrived;
    Map<String, Long> containerProcessed; //Currently not used;
    Map<String, Long> containerArrived; //Currently not used;
    Map<String, Double> containerUtilization;
    Map<String, Long> containerJobModelVersion;
    Set<String> containerIds;
    LeaderDispatcher dispatcher;
    public RMIMetricsRetriever(){
        rmiServer = new MetricsRetrieverRMIServer();
        taskProcessed = new HashMap<>();
        taskArrived = new HashMap<>();
        containerProcessed = new HashMap<>();
        containerArrived = new HashMap<>();
        containerUtilization = new HashMap<>();
        containerJobModelVersion = new HashMap<>();
        containerIds = new HashSet<>();
    }
    public void start(){
        rmiServer.start();
    }
    public void setJobModelVersions(Set<String> containerIds){
        for(String containerId: containerIds){
            containerJobModelVersion.put(containerId, -1l);
        }
    }
    public boolean checkMigrated(String srcId){
        MetricsClient client = new MetricsClient(rmiServer.getAddress(srcId), 8900 + Integer.parseInt(srcId), srcId);
        HashMap<String, String> offsets = client.getOffsets();
        long jobModelVersion = -1;
        if(offsets != null && offsets.containsKey("JobModelVersion")){
            jobModelVersion = Long.parseLong(offsets.get("JobModelVersion"));
        }
        //Update container JobModelVersion
        long oldJobModelVersion = containerJobModelVersion.getOrDefault(srcId, -1l);
        if(jobModelVersion > -1){
            if(jobModelVersion > oldJobModelVersion){
                return true;
            }
        }
        return false;
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
    public String getLeaderAddress(){return rmiServer.getLeaderAddress();}
    public void updateContainerIds(Set<String> containerIds){
        this.containerIds = containerIds;
    }
    public void retrieveMetrics(){
        HashMap<String, String> offsets;
        boolean isMigration = false;
        //timePoints.add(time);
        taskArrived.clear();
        taskProcessed.clear();
        containerArrived.clear();
        containerProcessed.clear();
        containerUtilization.clear();
        for(String containerId: containerIds){
            MetricsClient client = new MetricsClient(rmiServer.getAddress(containerId), 8900 + Integer.parseInt(containerId), containerId);
            offsets = client.getOffsets();
            double utilization = -100;
            long jobModelVersion = -1;
            //Update container JobModelVersion
            if(offsets != null && offsets.containsKey("JobModelVersion")){
                jobModelVersion = Long.parseLong(offsets.get("JobModelVersion"));
                offsets.remove("JobModelVersion");
            }
            long oldJobModelVersion = containerJobModelVersion.getOrDefault(containerId, -1l);
            if(jobModelVersion > -1 && jobModelVersion > oldJobModelVersion){
                containerJobModelVersion.put(containerId, jobModelVersion);
            }
            if(offsets != null && offsets.containsKey("Utilization")) {
                utilization = Double.parseDouble(offsets.get("Utilization"));
                offsets.remove("Utilization");
            }
            if(utilization > -1e-9){ //Online
                containerUtilization.put(containerId, utilization);
            }else { //Offline
                containerUtilization.put(containerId, 0.0);
            }

            long s_arrived = 0, s_processed = 0;
            for(Map.Entry<String, String> entry: offsets.entrySet()){
                String id = entry.getKey();
                String value = entry.getValue();
                int i = value.indexOf('_');
                long begin = rmiServer.getBeginOffset(id);
                long arrived = Long.parseLong(value.substring(0, i)) - begin - 1, processed = Long.parseLong(value.substring(i+1)) - begin;
                if(arrived < 0) arrived = 0;
                if(processed < 0) processed = 0;
                //delayEstimator.updatePartitionArrived(id, time, arrived);
                long t = taskArrived.getOrDefault(id, 0l);
                if(t > arrived) arrived = t;
                taskArrived.put(id, arrived);
                //delayEstimator.updatePartitionCompleted(id, time, processed);
                t = taskProcessed.getOrDefault(id, 0l);
                if(t > processed) processed = t;
                taskProcessed.put(id, processed);
                //delayEstimator.updatePartitionBacklog(id, time, containerId, arrived - processed);
                s_arrived += arrived;
                s_processed += processed;
            }
            containerArrived.put(containerId, s_arrived);
            containerProcessed.put(containerId, s_processed);
        }
        //return new MetricsMetadata(taskProcessed, taskArrived, containerProcessed, containerArrived, containerUtilization, containerJobModelVersion);

        /*
        //Raw information
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Arrived: " + containerArrived);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Processed: " + containerProcessed);
        */
    }
    public void setDispatcher(LeaderDispatcher dispatcher){
        this.dispatcher = dispatcher;
    }
    public boolean isApplicationRunning(){
        if(getLeaderAddress() != null) {
            this.dispatcher.updateLeaderAddress(getLeaderAddress() + ":1999");
            return true;
        }
        return false;
    }

}
