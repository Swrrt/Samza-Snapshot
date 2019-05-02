package org.apache.samza.clustermanager.dm;

import org.apache.samza.job.model.JobModel;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DMListenerEnforcer extends Remote {

    /**
     * Enforce the schema/parallelism sent by the dispatcher
     */
    void changeParallelism(int parallelism, JobModel jobModel) throws RemoteException;

    void rebalance(JobModel jobModel) throws RemoteException;

}
