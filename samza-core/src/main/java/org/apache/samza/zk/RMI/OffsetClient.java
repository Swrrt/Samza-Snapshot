package org.apache.samza.zk.RMI;

import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
    Sending and retrieving last processed offsets and begin offsets from Offset server.
 */
public class OffsetClient {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetClient.class);
    String leaderAddress = "";
    int port = 8884;
    String systemName = "";
    String topicName = "";
    public OffsetClient(){
    }
    public OffsetClient(String leaderAddress, int port, String systemName, String topicName){
        this.leaderAddress = leaderAddress;
        this.port = port;
        this.systemName = systemName;
        this.topicName = topicName;
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
        LOG.info("Sending processed offsets information to server");
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            OffsetMessage impl = (OffsetMessage) registry.lookup("myOffset");
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
        LOG.info("Retrieving processed offsets information from server");
        HashMap<String, Long> offsets = new HashMap<>();
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            OffsetMessage impl = (OffsetMessage) registry.lookup("myOffset");
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
        LOG.info("Sending begin offsets information to server");
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            OffsetMessage impl = (OffsetMessage) registry.lookup("myOffset");
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
        LOG.info("Retrieving begin offsets information from server");
        HashMap<String, Long> offsets = new HashMap<>();
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            OffsetMessage impl = (OffsetMessage) registry.lookup("myOffset");
            offsets = impl.getBegin();
            LOG.info("Begin offsets information: " + offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Begin offsets information got");
        return offsets;
    }
}

