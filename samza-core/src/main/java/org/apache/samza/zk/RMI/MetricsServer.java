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
    MetricsMessageImpl impl;
    int port = 8886;
    public MetricsServer(){
        metrics = new LinkedList<>();
    }
    public void setPort(int port){
        this.port = port;
    }
    public void register(String source, ReadableMetricsRegistry registry){
        if(source.startsWith("TaskName-Partition") || source.startsWith("samza-container-")) { // only send certain metrics
            metrics.add(new Pair<>(source, registry));
        }
    }
    public void start(){
        LOG.info("Metrics Server starting at port: " + port + "...");
        try{
            Registry registry = LocateRegistry.createRegistry(port);
            impl = new MetricsMessageImpl(metrics);
            registry.rebind("myMetrics", impl);
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("Metrics Server started");
    }
    public void clear(){
    }

    public void updateOffsets(ConcurrentHashMap beginOffset, ConcurrentHashMap lastProcessedOffset){
        impl.setOffset(beginOffset, lastProcessedOffset);
    }
    /*private void writeLog(String log){
        Calendar calendar = Calendar.getInstance();
        System.out.println(calendar.getTime() + " OffsetServer: " + log );
    }*/
}
