package org.apache.samza.zk.RMI;

import javafx.beans.binding.ObjectExpression;
import javafx.util.Pair;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
    This server runs on containers and provide arrived & processed information to DM.
 */
public class MetricsServer {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServer.class);
    List<Pair<String, ReadableMetricsRegistry>> metrics;
    HashMap<String, Long> processed;
    HashMap<String, Object> arrived;
    MetricsMessageImpl impl;
    int port = 8886;
    public MetricsServer(){
        metrics = new LinkedList<>();
        processed = new HashMap<>();
        arrived = new HashMap<>();
    }
    public void setPort(int port){
        this.port = port;
    }
    public void register(String source, ReadableMetricsRegistry registry){
        if(source.startsWith("TaskName-Partition") && registry.getGroups().contains("org.apache.samza.container.TaskInstanceMetrics") || source.startsWith("samza-container-") && registry.getGroups().contains("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics")) { // only send certain metrics
            metrics.add(new Pair<>(source, registry));
        }
    }
    public void start(){
        LOG.info("Metrics Server starting at port: " + port + "...");
        try{
            Registry registry = LocateRegistry.createRegistry(port);
            impl = new MetricsMessageImpl(metrics, arrived, processed);
            registry.rebind("myMetrics", impl);
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("Metrics Server started");
    }
    public void clear(){
        LOG.info("Clear metrics registries");
        metrics.clear();
    }

    /*public void updateOffsets(ConcurrentHashMap beginOffset, ConcurrentHashMap lastProcessedOffset){
        impl.setOffset(beginOffset, lastProcessedOffset);
    }*/
    public void setContainerModel(ContainerModel containerModel){
        LOG.info("Remove useless information based on container model");
        for(String id: arrived.keySet())
            if(!containerModel.getTasks().containsKey(id))arrived.remove(id);
        for(String id: processed.keySet())
            if(!containerModel.getTasks().containsKey(id))processed.remove(id);
    }
    /*public static ConcurrentHashMap<String, Long> translate(ConcurrentHashMap<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> offsets){
        ConcurrentHashMap<String, Long> newOffsets = new ConcurrentHashMap<>();
        for(Map.Entry<TaskName, ConcurrentHashMap<SystemStreamPartition, String>> t: offsets.entrySet()){
            String taskName = t.getKey().getTaskName();
            for(Map.Entry<SystemStreamPartition, String> tt : t.getValue().entrySet()){
                String offset = tt.getValue();
                newOffsets.put(taskName, Long.parseLong(offset));
            }
        }
        return newOffsets;
    }*/

    /*private void writeLog(String log){
        Calendar calendar = Calendar.getInstance();
        System.out.println(calendar.getTime() + " OffsetServer: " + log );
    }*/
}
