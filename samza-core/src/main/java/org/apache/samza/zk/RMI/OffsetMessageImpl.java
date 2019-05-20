package org.apache.samza.zk.RMI;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;

public class OffsetMessageImpl extends UnicastRemoteObject implements OffsetMessage {
    ConcurrentMap<String, Long> offsets;
    public OffsetMessageImpl() throws RemoteException{
    }
    public OffsetMessageImpl(ConcurrentMap<String, Long> offsets)throws RemoteException{
        this.offsets = offsets;
    }
    @Override
    public void send(HashMap offsets) throws RemoteException{
        this.offsets.putAll(offsets);
    }
    @Override
    public HashMap<String, Long> get()throws RemoteException{
        return new HashMap<>(offsets);
    }
}
