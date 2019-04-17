package org.apache.samza.job.dm;

import org.apache.samza.clustermanager.DMListenerEnforcer;
import org.apache.samza.config.Config;
import org.apache.samza.config.DMDispatcherConfig;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

//import org.apache.xmlrpc.*;

public class DefaultDispatcher implements DMDispatcher {
    private static final Logger LOG = Logger.getLogger(DefaultDispatcher.class.getName());

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
        LOG.info("dispatcher getenforcerfactory");
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
        // TODO: apply schema to the Enforcer;
        LOG.info("dispatcher enforce schema");

        // implementation for RMI based
        try {
            String url = enforcerURL.get(allocation.getStageID());
            DMListenerEnforcer enforcer = (DMListenerEnforcer) Naming.lookup("rmi://" + url + "/listener");
            enforcer.enforceSchema(allocation.getParallelism());
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        // -----------------------------

//        String stageId = allocation.getStageID();
//        Enforcer enf = getEnforcer(stageId);
//        enf.updateSchema(allocation);
    }

    @Override
    public void submitApplication(Allocation allocation) {
        LOG.info("dispatcher submit application");
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

}
