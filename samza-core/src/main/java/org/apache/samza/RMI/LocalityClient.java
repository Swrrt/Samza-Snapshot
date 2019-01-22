package org.apache.samza.RMI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class LocalityClient{
    private static final Logger LOG = LoggerFactory.getLogger(LocalityClient.class);
    String leaderAddress = "";
    int port = 8881;
    public LocalityClient(){
    }
    public LocalityClient(String leaderAddress, int port){
        this.leaderAddress = leaderAddress;
        this.port = port;
    }
    public void sendLocality(String processorId, String host){
        LOG.info("Sending locality information...");
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            LocalityMessage impl = (LocalityMessage) registry.lookup("myLocality");
            String message = processorId+'_'+host;
            LOG.info("Locality information: "+message);
            impl.send(message);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Locality information sent");
    }
}