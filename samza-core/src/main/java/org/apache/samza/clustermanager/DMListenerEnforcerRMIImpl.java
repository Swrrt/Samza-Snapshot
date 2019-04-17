package org.apache.samza.clustermanager;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DMListenerEnforcerRMIImpl extends UnicastRemoteObject implements DMListenerEnforcer {
    YarnApplicationMaster jc;
    DMListenerEnforcerRMIImpl(YarnApplicationMaster jc) throws RemoteException{
        super();
        this.jc = jc;
    }

    @Override
    public void enforceSchema(int parallelism) throws RemoteException {
        System.out.println("Receiving parallelism");
        System.out.println(parallelism);
        jc.scaleUpByN(parallelism);
    }
}
