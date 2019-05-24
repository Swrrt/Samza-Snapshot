package org.apache.samza.zk.RMI;

import javafx.util.Pair;
import org.apache.samza.metrics.ReadableMetricsRegistry;

import java.rmi.server.UnicastRemoteObject;
import java.util.List;

public class MetricsMessageImpl extends UnicastRemoteObject implements MetricsMessage {
    List<Pair<String, ReadableMetricsRegistry>> metrics;
    public MetricsMessageImpl(List<Pair<String, ReadableMetricsRegistry>> metrics){
        this.metrics = metrics;
    }
    Byte[] getMetrics(){

    }
}
