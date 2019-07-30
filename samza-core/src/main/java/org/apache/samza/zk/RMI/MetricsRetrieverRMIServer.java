package org.apache.samza.zk.RMI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


/*
    Stores containers' address information and partitions' begin offset, processed offset information at DM
    Containers will write their information to here by RMI.
 */

public class MetricsRetrieverRMIServer {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsRetrieverRMIServer.class);
    ConcurrentHashMap<String, Long> processedOffsets = null;
    ConcurrentHashMap<String, Long> beginOffsets = null;
    //To get shutting down time.
    ConcurrentHashMap<String, Long> shutdownTime = null;
    ConcurrentHashMap<String, Long> startTime = null;
    ConcurrentHashMap<String, String> address = null;
    public MetricsRetrieverRMIServer(){
        processedOffsets = new ConcurrentHashMap<>();
        beginOffsets = new ConcurrentHashMap<>();
        shutdownTime = new ConcurrentHashMap<>();
        startTime = new ConcurrentHashMap<>();
        address = new ConcurrentHashMap<>();
    }
    public void start(){
        LOG.info("Metrics Retriever RMI Server starting...");
        processedOffsets.clear();
        beginOffsets.clear();
        shutdownTime.clear();
        startTime.clear();
        address.clear();
        try{
            Registry registry = LocateRegistry.createRegistry(8881);
            registry.rebind("MetricsRetrieverRMIServer", new MetricsRetrieverRMIMessageImpl(processedOffsets, beginOffsets, shutdownTime, startTime, address));
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("Metrics Retriever RMI Server started");
    }
    public void clear(){
        processedOffsets.clear();
        beginOffsets.clear();
    }
    public long getBeginOffset(String id){
        return beginOffsets.get(id);
    }
    public void setShutdownTime(String Id, long time){
        shutdownTime.put(Id, time);
    }
    public void setStartTime(String Id, long time){
        startTime.put(Id, time);
    }
    public long getShutdownTime(String Id){
        return shutdownTime.get(Id);
    }
    public long getStartTime(String Id){
        return startTime.get(Id);
    }
    public HashMap getAndRemoveOffsets(){
        //Copy the offsets, in order to seclude local and remote resource
        HashMap<String, Long> temp = new HashMap<>();
        temp.putAll(processedOffsets);
        LOG.info("Got offsets: "+temp.toString());
        processedOffsets.clear();
        return temp;
    }
    public String getAddress(String processorId){
        return address.get(processorId);
    }
    public float getOffset(String partition){
        return processedOffsets.get(partition);
    }
}
