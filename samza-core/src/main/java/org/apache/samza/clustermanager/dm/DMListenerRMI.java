package org.apache.samza.clustermanager.dm;

import org.apache.samza.clustermanager.YarnApplicationMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class DMListenerRMI implements DMListener, Runnable {
    private static final Logger log = LoggerFactory.getLogger(DMListenerRMI.class);
    private YarnApplicationMaster jc;
    @Override
    public void registerToDM() {

    }

    @Override
    public void startListener() {
//        Thread thread = new Thread(server);
//        thread.setDaemon(true);
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void setYarnApplicationMaster(YarnApplicationMaster jc) {
        this.jc = jc;
    }

    @Override
    public void run() {
        log.info("Starting RMI server");
        try {
            DMListenerEnforcer enforcer = new DMListenerEnforcerRMIImpl(jc);
            LocateRegistry.createRegistry(1999);
            Naming.rebind("rmi://localhost:1999/listener", enforcer);

        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }


        log.info("RMI server starts up");
    }
}
