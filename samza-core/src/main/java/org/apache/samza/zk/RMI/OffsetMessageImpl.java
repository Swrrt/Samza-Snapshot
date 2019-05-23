package org.apache.samza.zk.RMI;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OffsetMessageImpl extends UnicastRemoteObject implements OffsetMessage {
    ConcurrentMap<String, Long> processedOffsets;
    ConcurrentMap<String, Long> beginOffsets;
    ConcurrentMap<String, Long> shutdownTime;
    ConcurrentMap<String, Long> startTime;
    public OffsetMessageImpl() throws RemoteException{
    }
    public OffsetMessageImpl(ConcurrentMap<String, Long> processedOffsets,
                             ConcurrentMap<String, Long> beginOffsets,
                             ConcurrentMap<String, Long> shutdownTime,
                             ConcurrentMap<String, Long> startTime)throws RemoteException{
        this.processedOffsets = processedOffsets;
        this.beginOffsets = beginOffsets;
        this.shutdownTime = shutdownTime;
        this.startTime = startTime;
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
}
