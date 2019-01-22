package org.apache.samza.RMI;
import org.apache.samza.RMI.UtilizationMessage;
import org.apache.samza.zk.FollowerJobCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalityServer {
    private static final Logger LOG = LoggerFactory.getLogger(LocalityServer.class);
    ConcurrentHashMap<String, String> locality = null;
    public LocalityServer(){
        locality = new ConcurrentHashMap<>();
    }
    public void start(){
        LOG.info("Locality Monitor starting...");
        try{
            Registry registry = LocateRegistry.createRegistry(8881);
            registry.rebind("myLocality", new UtilizationMessageImpl(locality));
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("Locality Monitor started");
    }
    public HashMap getLocalityMap(){
        //Copy the utilization, in order to seclude local and remote resource
        HashMap<String, String> temp = new HashMap<>();
        temp.putAll(locality);
        LOG.info("Got Loilicaty map: "+temp.toString());
        // TODO: need to be atomic?
        locality.clear();
        return temp;
    }
    public String getLocality(String processorId){
        return locality.get(processorId);
    }
}
