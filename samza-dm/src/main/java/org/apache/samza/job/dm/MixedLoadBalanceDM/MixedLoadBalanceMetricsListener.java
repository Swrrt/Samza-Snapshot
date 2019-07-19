package org.apache.samza.job.dm.MixedLoadBalanceDM;

import org.apache.samza.zk.RMI.LocalityServer;
import org.apache.samza.zk.RMI.MetricsClient;
import org.apache.samza.zk.RMI.OffsetServer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MixedLoadBalanceMetricsListener {
    public static MetricsMetadata retrieveArrivedAndProcessed(long time, Set<String> containerIds, LocalityServer localityServer, OffsetServer offsetServer, Map<String, Long> oldContainerJobModelVersion){
        HashMap<String, String> offsets;
        boolean isMigration = false;
        //timePoints.add(time);
        Map<String, Long> taskProcessed = new HashMap<>();
        Map<String, Long> taskArrived = new HashMap<>();
        Map<String, Long> containerProcessed = new HashMap<>();
        Map<String, Long> containerArrived = new HashMap<>();
        Map<String, Double> containerUtilization = new HashMap<>();
        Map<String, Long> containerJobModelVersion = oldContainerJobModelVersion;
        for(String containerId: containerIds){
            MetricsClient client = new MetricsClient(localityServer.getLocality(containerId), 8900 + Integer.parseInt(containerId), containerId);
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
                long begin = offsetServer.getBeginOffset(id);
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
        return new MetricsMetadata(taskProcessed, taskArrived, containerProcessed, containerArrived, containerUtilization, containerJobModelVersion);

        /*
        //Raw information
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Arrived: " + containerArrived);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Processed: " + containerProcessed);
        */
    }

}
