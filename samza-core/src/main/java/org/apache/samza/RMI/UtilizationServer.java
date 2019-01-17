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

public class UtilizationServer {
    private static final Logger LOG = LoggerFactory.getLogger(UtilizationServer.class);
    ConcurrentHashMap<String, Float> util = null;
    public UtilizationServer(){
        util = new ConcurrentHashMap<>();
    }
    public void start(){
        LOG.info("Utilization Monitor starting...");
        try{
            Registry registry = LocateRegistry.createRegistry(8883);
            registry.rebind("myUtilization", new UtilizationMessageImpl(util));
        }catch (Exception e){
            LOG.info("Excpetion happened: " + e.toString());
        }
        LOG.info("Utilization Monitor started");
    }
    public HashMap getAndRemoveUtilizationMap(){
        //Copy the utilization, in order to seclude local and remote resource
        HashMap<String, Float> temp = new HashMap<>();
        temp.putAll(util);
        LOG.info("Got utilization map: "+temp.toString());
        util.clear();
        return temp;
    }
    public float getUtilization(String processorId){
        return util.get(processorId);
    }
}
