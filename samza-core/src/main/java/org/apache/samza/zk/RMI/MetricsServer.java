package org.apache.samza.zk.RMI;

import javafx.util.Pair;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*
    This server runs on containers and provide arrived & processed information to DM.
 */
public class MetricsServer {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServer.class);
    List<Pair<String, ReadableMetricsRegistry>> metrics;
    public MetricsServer(){
        metrics = new LinkedList<>();
    }
    public void register(String source, ReadableMetricsRegistry registry){
        if(source.equals("")) { // only send certain metrics TODO
            metrics.add(new Pair<>(source, registry));
        }
    }
    public void start(){
        LOG.info("Metrics Server starting...");
        try{
            Registry registry = LocateRegistry.createRegistry(8886);
            registry.rebind("myMetrics", new MetricsMessageImpl(metrics));
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("Metrics Server started");
    }
    public void clear(){
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
