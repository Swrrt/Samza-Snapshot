package org.apache.samza.zk.MixedLoadBalancer.RMI;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

public class UtilizationMessageImpl extends UnicastRemoteObject implements UtilizationMessage {
    ConcurrentHashMap<String, Float> util = null;
    public UtilizationMessageImpl() throws RemoteException{
    }
    public UtilizationMessageImpl(ConcurrentHashMap util)throws RemoteException{
        this.util = util;
    }
    @Override
    public void send(String message) throws RemoteException{
        String processorId = message.substring(0, 6);
        float utilization = Float.parseFloat(message.substring(7));
        util.put(processorId, utilization);
    }
}
