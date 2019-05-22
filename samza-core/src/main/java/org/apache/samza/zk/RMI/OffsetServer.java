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
    public OffsetServer(){
        processedOffsets = new ConcurrentHashMap<>();
        beginOffsets = new ConcurrentHashMap<>();
    }
    public void start(){
        LOG.info("Last Processed Offsets Server starting...");
        processedOffsets.clear();
        beginOffsets.clear();
        try{
            Registry registry = LocateRegistry.createRegistry(8884);
            registry.rebind("myOffset", new OffsetMessageImpl(processedOffsets, beginOffsets));
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("OffsetServer started");
    }
    public void clear(){
        processedOffsets.clear();
        beginOffsets.clear();
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
