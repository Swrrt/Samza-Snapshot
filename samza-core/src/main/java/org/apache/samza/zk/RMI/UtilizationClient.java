package org.apache.samza.zk.RMI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class UtilizationClient{
    private static final Logger LOG = LoggerFactory.getLogger(UtilizationClient.class);
    String leaderAddress = "";
    int port = 8883;
    public UtilizationClient(){
    }
    public UtilizationClient(String leaderAddress, int port){
        this.leaderAddress = leaderAddress;
        this.port = port;
    }
    public void sendUtilization(String processorId, float util){
        LOG.info("Sending utilization information...");
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            UtilizationMessage impl = (UtilizationMessage) registry.lookup("myUtilization");
            String message = processorId+'_'+String.format("%.2f", util);
            LOG.info("Utilization information: "+message);
            impl.send(message);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Utilization information sent");
    }
}
