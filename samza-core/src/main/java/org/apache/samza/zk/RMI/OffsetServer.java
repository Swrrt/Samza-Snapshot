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
    ConcurrentHashMap<String, Long> offsets = null;
    public OffsetServer(){
        offsets = new ConcurrentHashMap<>();
    }
    public void start(){
        LOG.info("Last Processed Offsets Server starting...");
        try{
            Registry registry = LocateRegistry.createRegistry(8884);
            registry.rebind("myOffset", new OffsetMessageImpl(offsets));
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("OffsetServer started");
    }
    public void clear(){
        offsets.clear();
    }
    public HashMap getAndRemoveOffsets(){
        //Copy the offsets, in order to seclude local and remote resource
        HashMap<String, Long> temp = new HashMap<>();
        temp.putAll(offsets);
        LOG.info("Got offsets: "+temp.toString());
        offsets.clear();
        return temp;
    }
    public float getOffset(String partition){
        return offsets.get(partition);
    }
    /*private void writeLog(String log){
        Calendar calendar = Calendar.getInstance();
        System.out.println(calendar.getTime() + " OffsetServer: " + log );
    }*/
}
