package org.apache.samza.job.dm;

import org.apache.samza.config.Config;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class DMSchedulerListenerRMIImpl implements DMSchedulerListener, Runnable {

    DMScheduler scheduler;

    @Override
    public void startListener() {
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void setScheduler(DMScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void setConfig(Config config) {

    }

    @Override
    public void run() {
        try {
            DMSchedulerListenerRMIAPI listener = new DMScheduelrListenerRMIAPIImpl(scheduler);
            LocateRegistry.createRegistry(2000);
            Naming.rebind("rmi://127.0.0.1:2000/listener", listener);

        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }


        System.out.println("RMI server starts up");
    }
}
