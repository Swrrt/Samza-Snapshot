package org.apache.samza.util.RMI;

import org.apache.samza.clustermanager.YarnApplicationMaster;
import org.apache.samza.serializers.model.JobModelDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DecisionMessageImpl extends UnicastRemoteObject implements DecisionMessage {
    private static final Logger log = LoggerFactory.getLogger(DecisionMessageImpl.class);
    YarnApplicationMaster jc;
    DecisionMessageImpl(YarnApplicationMaster jc) throws RemoteException{
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
