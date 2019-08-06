package org.apache.samza.util.RMI;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

public interface MetricsMessage extends Remote{
    HashMap<String, String> getArrivedAndProcessed()throws RemoteException;
}
