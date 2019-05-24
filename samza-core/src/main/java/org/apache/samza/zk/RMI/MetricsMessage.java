package org.apache.samza.zk.RMI;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MetricsMessage extends Remote{
    Byte[] getMetrics()throws RemoteException;
}
