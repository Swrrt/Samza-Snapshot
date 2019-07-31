package org.apache.samza.job.dm.MixedLoadBalanceDM;

import org.apache.commons.logging.Log;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.samza.clustermanager.dm.DMListenerEnforcer;
import org.apache.samza.config.Config;
import org.apache.samza.config.DMDispatcherConfig;
import org.apache.samza.config.DMSchedulerConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.dm.Allocation;
import org.apache.samza.job.dm.DMDispatcher;
import org.apache.samza.job.dm.Enforcer;
import org.apache.samza.job.dm.EnforcerFactory;
import org.apache.samza.job.model.JobModel;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;


//import org.apache.xmlrpc.*;

public class DelayGuaranteeDispatcher {
    //private static final Logger LOG = Logger.getLogger(DelayGuaranteeDispatcher.class.getName());

    private ConcurrentMap<String, String> enforcerURL;

    public void init() {
        this.enforcerURL = new ConcurrentSkipListMap<String, String>();
    }

    public void updateJobModel(String stageId, JobModel jobModel){
        try {
            String url = enforcerURL.get(stageId);
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
    public void changeParallelism(String stageId, int parallelism, JobModel jobModel){
        try {
            String url = enforcerURL.get(stageId);
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

    public void updateEnforcerURL(String name, String url) {
        // TODO: update the Enforcer URL for later use of updateing paralellism
        enforcerURL.put(name, url);
    }
    private void writeLog(String log){
        System.out.println("DelayGuaranteeDispatcher: " + log);
    }

}
