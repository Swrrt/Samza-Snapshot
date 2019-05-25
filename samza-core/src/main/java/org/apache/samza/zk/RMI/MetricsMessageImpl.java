package org.apache.samza.zk.RMI;

import javafx.util.Pair;
import org.apache.samza.metrics.*;
import org.apache.samza.metrics.Timer;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsMessageImpl extends UnicastRemoteObject implements MetricsMessage {
    List<Pair<String, ReadableMetricsRegistry>> metrics;
    ConcurrentHashMap<String, Long> beginOffset, lastProcessedOffset;
    String topic;
    public MetricsMessageImpl(List<Pair<String, ReadableMetricsRegistry>> metrics)throws RemoteException {
        this.metrics = metrics;
        this.beginOffset = new ConcurrentHashMap<>();
        this.lastProcessedOffset = new ConcurrentHashMap<>();
    }
    public void setOffset(ConcurrentHashMap beginOffset, ConcurrentHashMap lastProcessedOffset){
        this.beginOffset.clear();
        this.beginOffset.putAll(beginOffset);
        this.lastProcessedOffset.clear();
        this.lastProcessedOffset.putAll(lastProcessedOffset);
    }
    public HashMap<String, String> getArrivedAndProcessed(){
        HashMap<String, String> ret = new HashMap<>();
        HashMap<String, Long> arrived = new HashMap<>(), processed = new HashMap<>();
        for(Pair<String, ReadableMetricsRegistry> pair: metrics){
            if(pair.getKey().startsWith("TaskName-Partition")){
                String id = pair.getKey().substring(9);
                if(pair.getValue().getGroups().contains("org.apache.samza.container.TaskInstanceMetrics")) { // Has processed metrics
                    pair.getValue().getGroup("org.apache.samza.container.TaskInstanceMetrics").get("messages-actually-processed").visit(new MetricsVisitor() {
                        @Override
                        public void counter(Counter counter) {
                            processed.put(id, counter.getCount());
                        }

                        @Override
                        public <T> void gauge(Gauge<T> gauge) {
                        }

                        @Override
                        public void timer(Timer timer) {
                        }
                    });
                }
            }else if(pair.getKey().startsWith("samza-container-")){ // Has arrived metrics
                if(pair.getValue().getGroups().contains("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics")){
                    for(Map.Entry<String, Metric> entry : pair.getValue().getGroup("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics").entrySet()){
                        String metricName = entry.getKey();
                        if(metricName.startsWith("kafka-" + topic.toLowerCase() + "-") && metricName.endsWith("-high-watermark")){
                            int i = ("kafka-" + topic.toLowerCase() + "-").length();
                            int j = metricName.indexOf('-', i);
                            String id = "Partition " + metricName.substring(i,j);
                            entry.getValue().visit(new MetricsVisitor() {
                                @Override
                                public void counter(Counter counter) {
                                }
                                @Override
                                public <T> void gauge(Gauge<T> gauge) {
                                    
                                }
                                @Override
                                public void timer(Timer timer) {
                                }
                            });
                        }
                    }
                }

            }
        }
        for(String id: lastProcessedOffset.keySet()){
            long arrive = 0;
            if(arrived.containsKey(id)){
                arrive = arrived.get(id);
            }
            arrive -= beginOffset.get(id);

            long processe = 0;
            if(processed.containsKey(id)){
                processe = processed.get(id);
            }
            processe += lastProcessedOffset.get(id) - beginOffset.get(id);
            ret.put(id, arrive + "_" + processe);
        }
        return ret;
    }
}
