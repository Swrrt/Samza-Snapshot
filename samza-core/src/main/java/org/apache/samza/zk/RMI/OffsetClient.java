package org.apache.samza.zk.RMI;

import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
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
    public void sendOffset(ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> lastProcessedOffset){
        HashMap<String, Long> offsets = new HashMap<>();
        for(Map.Entry<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> t: lastProcessedOffset.entrySet()){
            String taskName = t.getKey().getTaskName();
            for(Map.Entry<SystemStreamPartition, String> tt : t.getValue().entrySet()){
                String offset = tt.getValue();
                offsets.put(taskName, Long.parseLong(offset));
            }
        }
        sendOffset(offsets);
    }
    public void sendOffset(HashMap<String, Long> offsets){
        LOG.info("Sending Offsets information to server");
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            OffsetMessage impl = (OffsetMessage) registry.lookup("myOffset");
            LOG.info("Offsets information: " + offsets);
            impl.send(offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Offset information sent");
    }
    public ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> getLastProcessedOffset(){
        HashMap<String, Long> offsets = getOffset();
        ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> lastProcessedOffset = new ConcurrentHashMap<>();
        for (String i : offsets.keySet()){
            TaskName taskName = new TaskName(i);
            lastProcessedOffset.put(taskName, new ConcurrentHashMap<>());
            lastProcessedOffset.get(taskName).put(new SystemStreamPartition(systemName, topicName, new Partition(Integer.valueOf(i.substring(10)))), offsets.get(i).toString());
        }
        return lastProcessedOffset;
    }
    public HashMap<String, Long> getOffset(){
        LOG.info("Retrieving Offsets information from server");
        HashMap<String, Long> offsets = new HashMap<>();
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            OffsetMessage impl = (OffsetMessage) registry.lookup("myOffset");
            offsets = impl.get();
            LOG.info("Offsets information: " + offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Offsets information got");
        return offsets;
    }
}
