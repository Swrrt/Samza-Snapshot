package org.apache.samza.util.RMI;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;

public class MetricsRetrieverRMIMessageImpl extends UnicastRemoteObject implements MetricsRetrieverRMIMessage {
    ConcurrentMap<String, Long> processedOffsets;
    ConcurrentMap<String, Long> beginOffsets;
    ConcurrentMap<String, Long> shutdownTime;
    ConcurrentMap<String, Long> startTime;
    ConcurrentMap<String, String> address;
    public MetricsRetrieverRMIMessageImpl() throws RemoteException {
    }
    public MetricsRetrieverRMIMessageImpl(ConcurrentMap<String, Long> processedOffsets,
                             ConcurrentMap<String, Long> beginOffsets,
                             ConcurrentMap<String, Long> shutdownTime,
                             ConcurrentMap<String, Long> startTime, ConcurrentMap<String, String> address)throws RemoteException{
        this.processedOffsets = processedOffsets;
        this.beginOffsets = beginOffsets;
        this.shutdownTime = shutdownTime;
        this.startTime = startTime;
        this.address = address;
    }
    @Override
    public void sendProcessed(HashMap offsets) throws RemoteException{
        this.processedOffsets.putAll(offsets);
    }
    @Override
    public HashMap<String, Long> getProcessed()throws RemoteException{
        return new HashMap<>(processedOffsets);
    }
    @Override
    public void sendBegin(HashMap offsets) throws RemoteException{
        this.beginOffsets.putAll(offsets);
    }
    @Override
    public HashMap<String, Long> getBegin()throws RemoteException{
        return new HashMap<>(beginOffsets);
    }
    @Override
    public void sendShutdownTime(String containerId, long time) throws RemoteException{
        this.shutdownTime.put(containerId, time);
    }
    @Override
    public void sendStartTime(String containerId, long time) throws RemoteException{
        this.startTime.put(containerId, time);
    }
    @Override
    public void sendAddress(String message) throws RemoteException{
        String processorId = message.substring(0, 6);
        String host = message.substring(7);
        this.address.put(processorId, host);
    }
}
