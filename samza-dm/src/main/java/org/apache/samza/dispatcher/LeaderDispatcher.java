package org.apache.samza.dispatcher;

import org.apache.samza.clustermanager.dm.DMListenerEnforcer;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.JobModelSerializer;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;


//import org.apache.xmlrpc.*;

public class LeaderDispatcher {
    //private static final Logger LOG = Logger.getLogger(DelayGuaranteeDispatcher.class.getName());

    private String leaderAddress;

    public void init() {
        this.leaderAddress = null;
    }

    public void updateJobModel(JobModel jobModel){
        try {
            String url = leaderAddress;
            DMListenerEnforcer enforcer = (DMListenerEnforcer) Naming.lookup("rmi://" + url + "/listener");
            enforcer.rebalance(JobModelSerializer.jobModelToString(jobModel));
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
    public void changeParallelism(int parallelism, JobModel jobModel){
        try {
            String url = leaderAddress;
            DMListenerEnforcer enforcer = (DMListenerEnforcer) Naming.lookup("rmi://" + url + "/listener");
            enforcer.changeParallelism(parallelism, JobModelSerializer.jobModelToString(jobModel));
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
    public void updateLeaderAddress(String url){
        leaderAddress = url;
    }
    public boolean okToDispatch(){
        return leaderAddress != null;
    }
    private void writeLog(String log){
        System.out.println("DelayGuaranteeDispatcher: " + log);
    }

}
