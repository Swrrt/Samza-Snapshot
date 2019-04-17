package org.apache.samza.clustermanager;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DMListenerEnforcer extends Remote {

    /**
     * Enforce the schema/parallelism sent by the dispatcher
     */
    void enforceSchema(int parallelism) throws RemoteException;

}
