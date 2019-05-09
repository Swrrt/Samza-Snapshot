package org.apache.samza.clustermanager.dm;

import org.apache.samza.clustermanager.YarnApplicationMaster;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DMListenerEnforcerRMIImpl extends UnicastRemoteObject implements DMListenerEnforcer {
    private static final Logger log = LoggerFactory.getLogger(DMListenerEnforcerRMIImpl.class);
    YarnApplicationMaster jc;
    DMListenerEnforcerRMIImpl(YarnApplicationMaster jc) throws RemoteException{
        super();
        this.jc = jc;
    }

    @Override
    public void changeParallelism(int parallelism, String jobModelString) throws RemoteException {
        log.info("Receiving parallelism: " + parallelism);
        jc.scaleToN(parallelism, JobModelDeserializer.deserializeJobModel(jobModelString));
    }

    @Override
    public void rebalance(String jobModelString) throws RemoteException {
        log.info("rebalancing to JobModel");
        jc.enforceJobModel(JobModelDeserializer.deserializeJobModel(jobModelString));
    }
}
