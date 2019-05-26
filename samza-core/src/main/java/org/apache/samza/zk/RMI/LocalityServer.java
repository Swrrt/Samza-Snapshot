package org.apache.samza.zk.RMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class LocalityServer {
    //private static final Logger LOG = LoggerFactory.getLogger(LocalityServer.class);
    ConcurrentHashMap<String, String> locality = null;
    public LocalityServer(){
        locality = new ConcurrentHashMap<>();
    }
    public void start(){
        writeLog("Locality Monitor starting...");
        try{
            Registry registry = LocateRegistry.createRegistry(8881);
            registry.rebind("myLocality", new LocalityMessageImpl(locality));
        }catch (Exception e){
            writeLog("Excpetion happened: " + e.toString());
        }
        writeLog("Locality Monitor started");
    }
    public HashMap getLocalityMap(){
        //Copy the utilization, in order to seclude local and remote resource
        HashMap<String, String> temp = new HashMap<>();
        temp.putAll(locality);
        writeLog("Got Loilicaty map: "+temp.toString());
        // TODO: need to be atomic?
        locality.clear();
        return temp;
    }
    public String getLocality(String processorId){
        return locality.get(processorId);
    }
    public HashMap<String, String> getLocalities(){
        HashMap<String, String> local = new HashMap<>();
        local.putAll(locality);
        return local;
    }
    private void writeLog(String log){
        System.out.println(log);
    }
}
