package org.apache.samza.zk.RMI;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

public interface MetricsRetrieverRMIMessage extends Remote {
    void sendProcessed(HashMap<String, Long> message) throws RemoteException;
    void sendBegin(HashMap<String, Long> message) throws RemoteException;
    HashMap<String, Long> getProcessed() throws RemoteException;
    HashMap<String, Long> getBegin() throws RemoteException;
    void sendShutdownTime(String containerId, long timeStamp)throws RemoteException;
    void sendStartTime(String containerId, long timeStamp)throws RemoteException;
    void sendAddress(String message) throws RemoteException;
}
