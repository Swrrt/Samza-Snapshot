package org.apache.samza.zk.RMI;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public interface MetricsMessage extends Remote{
    HashMap<String, String> getArrivedAndProcessed()throws RemoteException;
}
