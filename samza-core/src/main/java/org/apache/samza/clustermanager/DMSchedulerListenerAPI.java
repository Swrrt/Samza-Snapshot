package org.apache.samza.clustermanager;

import java.rmi.Remote;

public interface DMSchedulerListenerAPI extends Remote {

    public void updateWorkload(String data);
}
