package org.apache.samza.util.RMI;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DecisionMessage extends Remote {

    /**
     * Enforce the schema/parallelism sent by the dispatcher
     */
    void changeParallelism(int parallelism, String jobModelString) throws RemoteException;

    void rebalance(String jobModelString) throws RemoteException;


}
