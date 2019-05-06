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

public class MixedLoadBalanceDispatcher implements DMDispatcher {
    //private static final Logger LOG = Logger.getLogger(MixedLoadBalanceDispatcher.class.getName());

    private ConcurrentMap<String, Enforcer> enforcers;
    private ConcurrentMap<String, String> enforcerURL;
    private Config config;
    private DMDispatcherConfig dispatcherConfig;

    @Override
    public void init(Config config) {
        this.config = config;
        this.dispatcherConfig = new DMDispatcherConfig(config);
        this.enforcers = new ConcurrentSkipListMap<String, Enforcer>();
        this.enforcerURL = new ConcurrentSkipListMap<String, String>();
    }

    @Override
    public EnforcerFactory getEnforcerFactory(String stage) {
        writeLog("dispatcher get Enforcerfactory");
        String EnforcerFactoryClass = "YarnJobFactory";
        if (config.containsKey("dm.enforcerfactory." + stage)) {
            EnforcerFactoryClass = config.get("dm.enforcerfactory." + stage, "YarnEnforcerFactory");
        }
        EnforcerFactory enforcerFactory = null;
        try {
            enforcerFactory = (EnforcerFactory) Class.forName(EnforcerFactoryClass).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return enforcerFactory;
    }

    @Override
    public Enforcer getEnforcer(String stageId) {
        return enforcers.get(stageId);
    }

    @Override
    public void enforceSchema(Allocation allocation) {
    }

    public void updateJobModel(Allocation allocation, JobModel jobModel){
        try {
            String url = enforcerURL.get(allocation.getStageID());
            DMListenerEnforcer enforcer = (DMListenerEnforcer) Naming.lookup("rmi://" + url + "/listener");
            enforcer.rebalance(jobModel);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
    public void changeParallelism(Allocation allocation, int parallelism, JobModel jobModel){
        try {
            String url = enforcerURL.get(allocation.getStageID());
            DMListenerEnforcer enforcer = (DMListenerEnforcer) Naming.lookup("rmi://" + url + "/listener");
            enforcer.changeParallelism(parallelism, jobModel);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void submitApplication(Allocation allocation) {
        writeLog("dispatcher submit application");
        String stageId = allocation.getStageID();
        EnforcerFactory enfFac = getEnforcerFactory(stageId);
        Enforcer enf = enfFac.getEnforcer(config);
        enforcers.put(stageId, enf);
        enf.submit();
    }

    public void updateEnforcerURL(String name, String url) {
        // TODO: update the Enforcer URL for later use of updateing paralellism
        enforcerURL.put(name, url);
    }
    private void writeLog(String log){
        System.out.println("MixedLoadBalanceDispatcher: " + log);
    }

}
