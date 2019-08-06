package org.apache.samza.util.RMI;

import com.google.common.util.concurrent.AtomicDouble;
import javafx.util.Pair;
import org.apache.samza.metrics.*;
import org.apache.samza.metrics.Timer;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsMessageImpl extends UnicastRemoteObject implements MetricsMessage {
    List<Pair<String, ReadableMetricsRegistry>> metrics;
    //ConcurrentHashMap<String, Long> beginOffset, lastProcessedOffset;
    ConcurrentHashMap<String, Object> arrived;
    ConcurrentHashMap<String, Long> processed;
    AtomicDouble utilization;
    AtomicLong jobModelVersion;
    String topic;
    public MetricsMessageImpl(List<Pair<String, ReadableMetricsRegistry>> metrics, ConcurrentHashMap<String, Object> arrived, ConcurrentHashMap<String, Long> processed, AtomicDouble utilization, AtomicLong jobModelVersion, String topic)throws RemoteException {
        this.metrics = metrics;
        this.arrived = arrived;
        this.processed = processed;
        this.topic = topic;
        this.utilization = utilization;
        this.jobModelVersion = jobModelVersion;
        //this.beginOffset = new ConcurrentHashMap<>();
        //this.lastProcessedOffset = new ConcurrentHashMap<>();
    }
    /*public void setOffset(ConcurrentHashMap beginOffset, ConcurrentHashMap lastProcessedOffset){
        this.beginOffset.clear();
        this.beginOffset.putAll(beginOffset);
        this.lastProcessedOffset.clear();
        this.lastProcessedOffset.putAll(lastProcessedOffset);
    }*/
    public HashMap<String, String> getArrivedAndProcessed(){
        HashMap<String, String> ret = new HashMap<>();
        //System.out.println("Metrics: " + metrics);
        for(Pair<String, ReadableMetricsRegistry> pair: metrics){
            if(pair.getKey().startsWith("TaskName-Partition")){
                String id = pair.getKey().substring(9);
                if(pair.getValue().getGroups().contains("org.apache.samza.container.TaskInstanceMetrics")) { // Has processed metrics
                    //          System.out.println(pair.getValue().getGroup("org.apache.samza.container.TaskInstanceMetrics"));
                    if (pair.getValue().getGroup("org.apache.samza.container.TaskInstanceMetrics").containsKey("messages-total-processed")) {
                        pair.getValue().getGroup("org.apache.samza.container.TaskInstanceMetrics").get("messages-total-processed").visit(new MetricsVisitor() {
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
                }
            }else if(pair.getKey().startsWith("samza-container-")){ // Has arrived metrics
                //System.out.println("samza-container- : " + pair.getValue().getGroups());
                if(pair.getValue().getGroups().contains("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics")){
            //        System.out.println("samza-container- : " + pair.getValue().getGroup("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics"));
                    for(Map.Entry<String, Metric> entry : pair.getValue().getGroup("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics").entrySet()){
                        String metricName = entry.getKey();
                        if(metricName.startsWith("kafka-" + topic.toLowerCase() + "-") && metricName.endsWith("-high-watermark") && !metricName.endsWith("behind-high-watermark")){
                            int i = ("kafka-" + topic.toLowerCase() + "-").length();
                            int j = metricName.indexOf('-', i);
                            String id = "Partition " + metricName.substring(i,j);
                            entry.getValue().visit(new MetricsVisitor() {
                                @Override
                                public void counter(Counter counter) {
                                }
                                @Override
                                public <T> void gauge(Gauge<T> gauge) {
                                    if(Long.parseLong(gauge.toString()) != -1) {
                                        arrived.put(id, gauge);
                                    }
                                }
                                @Override
                                public void timer(Timer timer) {
                                }
                            });
                        }
                    }
                }

                if(pair.getValue().getGroups().contains("org.apache.samza.container.SamzaContainerMetrics")) {
                    if (pair.getValue().getGroup("org.apache.samza.container.SamzaContainerMetrics").containsKey("average-utilization")) {
                        pair.getValue().getGroup("org.apache.samza.container.SamzaContainerMetrics").get("average-utilization").visit(new MetricsVisitor() {
                            @Override
                            public void counter(Counter counter) {
                            }

                            @Override
                            public <T> void gauge(Gauge<T> gauge) {
                                utilization.set(Double.parseDouble(gauge.toString()));
                            }

                            @Override
                            public void timer(Timer timer) {
                            }
                        });
                    }
                }
            }
        }
        //System.out.println("Arrived: " + arrived);
        //System.out.println("Processed: " + processed);
        for(String id: arrived.keySet()){
            long arrive = 0;
            if(arrived.containsKey(id)){
                arrive = Long.parseLong(arrived.get(id).toString());
            }
            //arrive -= beginOffset.get(id);

            long processe = 0;
            if(processed.containsKey(id)) {
                processe = processed.get(id);
                //processe += lastProcessedOffset.get(id) - beginOffset.get(id);
                if(arrive <= processe){ //Arrival not updated yet
                    arrive = processe + 1;
                }
                ret.put(id, arrive + "_" + processe);
            }
        }
        ret.put("Utilization", utilization.toString());
        ret.put("JobModelVersion", jobModelVersion.toString());
        System.out.println("Return: " + ret);
        return ret;
    }
}