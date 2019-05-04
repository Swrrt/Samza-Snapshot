package org.apache.samza.zk.RMI;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

public class LocalityMessageImpl extends UnicastRemoteObject implements UtilizationMessage {
    ConcurrentHashMap<String, String> locality = null;
    public LocalityMessageImpl() throws RemoteException{
    }
    public LocalityMessageImpl(ConcurrentHashMap util)throws RemoteException{
        this.locality = util;
    }
    @Override
    public void send(String message) throws RemoteException{
        String processorId = message.substring(0, 6);
        String host = message.substring(7);
        locality.put(processorId, host);
    }
}
