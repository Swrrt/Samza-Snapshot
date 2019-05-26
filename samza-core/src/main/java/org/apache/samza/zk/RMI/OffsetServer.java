package org.apache.samza.zk.RMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

//A place to store last processed offset of partitions.
//Currently runs in DM(MixedLoadBalance). Could run in AM(Leader)
public class OffsetServer {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetServer.class);
    ConcurrentHashMap<String, Long> processedOffsets = null;
    ConcurrentHashMap<String, Long> beginOffsets = null;
    //To get shutting down time.
    ConcurrentHashMap<String, Long> shutdownTime = null;
    ConcurrentHashMap<String, Long> startTime = null;
    public OffsetServer(){
        processedOffsets = new ConcurrentHashMap<>();
        beginOffsets = new ConcurrentHashMap<>();
        shutdownTime = new ConcurrentHashMap<>();
        startTime = new ConcurrentHashMap<>();
    }
    public void start(){
        LOG.info("Last Processed Offsets Server starting...");
        processedOffsets.clear();
        beginOffsets.clear();
        shutdownTime.clear();
        startTime.clear();
        try{
            Registry registry = LocateRegistry.createRegistry(8884);
            registry.rebind("myOffset", new OffsetMessageImpl(processedOffsets, beginOffsets, shutdownTime, startTime));
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("OffsetServer started");
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
    public float getOffset(String partition){
        return processedOffsets.get(partition);
    }
    /*private void writeLog(String log){
        Calendar calendar = Calendar.getInstance();
        System.out.println(calendar.getTime() + " OffsetServer: " + log );
    }*/
}
