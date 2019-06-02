package org.apache.samza.zk.RMI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsClient {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsClient.class);
    String leaderAddress = "";
    int port = 8884;
    public MetricsClient(String leaderAddress, int port){
        this.leaderAddress = leaderAddress;
        this.port = port;
    }
    public HashMap<String, String> getOffsets(){
        LOG.info("Retrieving processed offsets information from server " + leaderAddress);
        HashMap<String, String> offsets = new HashMap<>();
        try{
            Registry registry = LocateRegistry.getRegistry(leaderAddress, port);
            MetricsMessage impl = (MetricsMessage) registry.lookup("myMetrics");
            offsets = impl.getArrivedAndProcessed();
            LOG.info("Processed offsets information: " + offsets);
        }catch (Exception e){
            LOG.info("Exception happened: "+ e.toString());
        }
        LOG.info("Processed and arrived offsets information got");
        return offsets;
    }
    public static void main(String args[]){
        MetricsClient metricsClient = new MetricsClient(args[0], Integer.parseInt(args[1]));
        HashMap <String, String> offsets;
        while(true){
            try{
                Thread.sleep(5000);
            }catch (Exception e){
            }
            System.out.println("Try to retrieve offsets...");
            offsets = metricsClient.getOffsets();
            System.out.println("Offset is : " + offsets);
        }
    }
}
