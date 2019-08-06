package org.apache.samza.util.RMI;

import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsRetrieverRMIClient {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsRetrieverRMIClient.class);
    String DMAddress = "";
    int port = 8881;
    String systemName = "";
    String topicName = "";
    boolean debugInfo = true;
    public MetricsRetrieverRMIClient(){
    }
    public MetricsRetrieverRMIClient(String DMAddress, int port, String systemName, String topicName, boolean debug){
        debugInfo = debug;
        this.DMAddress = DMAddress;
        this.port = port;
        this.systemName = systemName;
        this.topicName = topicName;
    }
    public MetricsRetrieverRMIClient(String DMAddress, int port, String systemName, String topicName){
        this(DMAddress, port, systemName, topicName, true);
    }
    public void sendProcessedOffset(ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> lastProcessedOffset){
        HashMap<String, Long> offsets = new HashMap<>();
        for(Map.Entry<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> t: lastProcessedOffset.entrySet()){
            String taskName = t.getKey().getTaskName();
            for(Map.Entry<SystemStreamPartition, String> tt : t.getValue().entrySet()){
                String offset = tt.getValue();
                offsets.put(taskName, Long.parseLong(offset));
            }
        }
        sendProcessedOffset(offsets);
    }
    public void sendProcessedOffset(HashMap<String, Long> offsets){
        LOG.info("Sending processed offsets information to DM");
        try{
            Registry registry = LocateRegistry.getRegistry(DMAddress, port);
            MetricsRetrieverRMIMessage impl = (MetricsRetrieverRMIMessage) registry.lookup("MetricsRetrieverRMIServer");
            LOG.info("Processed offsets information: " + offsets);
            impl.sendProcessed(offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Processed offset information sent");
    }
    public ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> getLastProcessedOffset(ContainerModel containerModel){
        HashMap<String, Long> offsets = getProcessedOffset();
        ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> lastProcessedOffset = new ConcurrentHashMap<>();
        for (String i : offsets.keySet()){
            TaskName taskName = new TaskName(i);
            if(containerModel.getTasks().containsKey(taskName)) {
                lastProcessedOffset.put(taskName, new ConcurrentHashMap<>());
                lastProcessedOffset.get(taskName).put(new SystemStreamPartition(systemName, topicName, new Partition(Integer.valueOf(i.substring(10)))), offsets.get(i).toString());
            }
        }
        return lastProcessedOffset;
    }
    public HashMap<String, Long> getProcessedOffset(){
        LOG.info("Retrieving processed offsets information from DM");
        HashMap<String, Long> offsets = new HashMap<>();
        try{
            Registry registry = LocateRegistry.getRegistry(DMAddress, port);
            MetricsRetrieverRMIMessage impl = (MetricsRetrieverRMIMessage) registry.lookup("MetricsRetrieverRMIServer");
            offsets = impl.getProcessed();
            LOG.info("Processed offsets information: " + offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Processed offsets information got");
        return offsets;
    }


    public void sendBeginOffset(ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> beginOffset){
        HashMap<String, Long> offsets = new HashMap<>();
        for(Map.Entry<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> t: beginOffset.entrySet()){
            String taskName = t.getKey().getTaskName();
            for(Map.Entry<SystemStreamPartition, String> tt : t.getValue().entrySet()){
                String offset = tt.getValue();
                offsets.put(taskName, Long.parseLong(offset));
            }
        }
        sendBeginOffset(offsets);
    }
    public void sendBeginOffset(HashMap<String, Long> offsets){
        LOG.info("Sending begin offsets information to DM");
        try{
            Registry registry = LocateRegistry.getRegistry(DMAddress, port);
            MetricsRetrieverRMIMessage impl = (MetricsRetrieverRMIMessage) registry.lookup("MetricsRetrieverRMIServer");
            LOG.info("Begin offsets information: " + offsets);
            impl.sendBegin(offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Begin offset information sent");
    }
    public ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> getBeginOffset(ContainerModel containerModel){
        HashMap<String, Long> offsets = getBeginOffset();
        ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> beginOffset = new ConcurrentHashMap<>();
        for (String i : offsets.keySet()){
            TaskName taskName = new TaskName(i);
            if(containerModel.getTasks().containsKey(taskName)) {
                beginOffset.put(taskName, new ConcurrentHashMap<>());
                beginOffset.get(taskName).put(new SystemStreamPartition(systemName, topicName, new Partition(Integer.valueOf(i.substring(10)))), offsets.get(i).toString());
            }
        }
        return beginOffset;
    }
    public HashMap<String, Long> getBeginOffset(){
        LOG.info("Retrieving begin offsets information from DM");
        HashMap<String, Long> offsets = new HashMap<>();
        try{
            Registry registry = LocateRegistry.getRegistry(DMAddress, port);
            MetricsRetrieverRMIMessage impl = (MetricsRetrieverRMIMessage) registry.lookup("MetricsRetrieverRMIServer");
            offsets = impl.getBegin();
            LOG.info("Begin offsets information: " + offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Begin offsets information got");
        return offsets;
    }

    // Sending container shutdown timestamp
    public void sendShutdownTime(String containerId, long time){
        LOG.info("Send shutdown timestamp to DM");
        try{
            Registry registry = LocateRegistry.getRegistry(DMAddress, port);
            MetricsRetrieverRMIMessage impl = (MetricsRetrieverRMIMessage) registry.lookup("MetricsRetrieverRMIServer");
            LOG.info("Container " + containerId + " shutdown timestamp:" + time);
            impl.sendShutdownTime(containerId, time);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
    }
    public void sendStartTime(String containerId, long time){
        LOG.info("Send start timestamp to DM");
        try{
            Registry registry = LocateRegistry.getRegistry(DMAddress, port);
            MetricsRetrieverRMIMessage impl = (MetricsRetrieverRMIMessage) registry.lookup("MetricsRetrieverRMIServer");
            LOG.info("Container " + containerId + " start timestamp:" + time);
            impl.sendShutdownTime(containerId, time);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
    }
    public void sendAddress(String processorId, String host){
        LOG.info("Sending address information to " + DMAddress +":"+ port +" ...");
        try{
            Registry registry = LocateRegistry.getRegistry(DMAddress, port);
            MetricsRetrieverRMIMessage impl = (MetricsRetrieverRMIMessage) registry.lookup("MetricsRetrieverRMIServer");
            String message = processorId+'_'+host;
            LOG.info("Address information: "+message);
            impl.sendAddress(message);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Address information sent");
    }
}
