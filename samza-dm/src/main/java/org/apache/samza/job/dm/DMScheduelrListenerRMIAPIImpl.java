package org.apache.samza.job.dm;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DMScheduelrListenerRMIAPIImpl extends UnicastRemoteObject implements DMSchedulerListenerRMIAPI {
    DMScheduler scheduler;

    protected DMScheduelrListenerRMIAPIImpl(DMScheduler scheduler) throws RemoteException {
        this.scheduler = scheduler;
    }

    @Override
    public void updateWorkload(String data) {
        System.out.println(data);
    }
}
