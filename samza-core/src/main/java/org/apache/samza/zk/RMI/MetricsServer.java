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
    ConcurrentHashMap<String, Long> processed;
    ConcurrentHashMap<String, Object> arrived;
    MetricsMessageImpl impl;
    String topic = "";
    int port = 8886;
    public MetricsServer(){
        metrics = new LinkedList<>();
        processed = new ConcurrentHashMap<>();
        arrived = new ConcurrentHashMap<>();
    }
    public void setPort(int port){
        this.port = port;
    }
    public void setTopic(String topic){this.topic = topic;}
    public void register(String source, ReadableMetricsRegistry registry){
        if(source.startsWith("TaskName-Partition") && registry.getGroups().contains("org.apache.samza.container.TaskInstanceMetrics") || source.startsWith("samza-container-") && registry.getGroups().contains("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics")) { // only send certain metrics
            LOG.info("Registering " + source + " to MetricsServer");
            metrics.add(new Pair<>(source, registry));
        }
    }
    public void start(){
        LOG.info("Metrics Server starting at port: " + port + " with topic: " + topic + "...");
        try{
            Registry registry = LocateRegistry.createRegistry(port);
            impl = new MetricsMessageImpl(metrics, arrived, processed, topic);
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
        LOG.info("Remove useless information based on container model: " + containerModel.getTasks().keySet());
        for(String id: arrived.keySet()) {
            if (!containerModel.getTasks().containsKey(new TaskName(id))){
                arrived.remove(id);
                LOG.info("Remove " + id + "offsets");
            }
        }
        for(String id: processed.keySet()) {
            if (!containerModel.getTasks().containsKey(new TaskName(id))) {
                processed.remove(id);
                LOG.info("Remove " + id + "offsets");
            }
        }
        /*for(TaskName id: containerModel.getTasks().keySet()){
            if(!arrived.containsKey(id)){
                arrived.put()
            }
        }*/
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
