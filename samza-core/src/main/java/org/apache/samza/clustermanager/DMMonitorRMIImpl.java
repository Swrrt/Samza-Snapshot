package org.apache.samza.clustermanager;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class DMMonitorRMIImpl implements DMMonitor {
    @Override
    public void registerContainer(String containerId) {

    }

    @Override
    public void deregisterContainer(String containerId) {

    }

    @Override
    public void updateResourceAndLoad() {
        try {
            DMSchedulerListenerAPI listenerAPI = (DMSchedulerListenerAPI) Naming.lookup("rmi://127.0.0.1:2000/listener");
            listenerAPI.updateWorkload("resources");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getClusterResource() {

    }

    @Override
    public void getContainerLoad() {

    }
}
