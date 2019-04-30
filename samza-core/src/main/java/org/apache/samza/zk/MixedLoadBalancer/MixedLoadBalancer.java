package org.apache.samza.zk.MixedLoadBalancer;

import org.apache.samza.zk.MixedLoadBalancer.RMI.UtilizationClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
    This class periodically check the load and then try to rebalance the load.
 */
public class MixedLoadBalancer implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(JVMMonitor.class);
    private final int SLEEP_INTERVAL = 1000;
    private Thread t;
    private MixedLoadBalanceManager mixedLoadBalanceManager;
    public void run(){
        LOG.info("Running mixed load balancer");
        mixedLoadBalanceManager = new MixedLoadBalanceManager();

        try{
            while(true){
                Thread.sleep(SLEEP_INTERVAL);
                //TODO: Try to rebalance load
                if(!mixedLoadBalanceManager.checkLoad()) {
                    LOG.info("Imbalance load detected, try to rebalance");
                    mixedLoadBalanceManager.rebalanceJobModel();
                }
            }
        }catch(Exception e){
            LOG.info("Exception happens: "+e.toString());
        }
        LOG.info("Mixed load balancer stopped");
    }
    public void start(String leaderAddr, String processorId){
        LOG.info("Starting mixed load balancer:" +leaderAddr);
        if(t == null){
            t = new Thread(this, "JVM monitor");
            t.start();
        }
    }
    public void stop(){
        try{
            if(t!=null)t.join();
        }catch(Exception e){
            LOG.error(e.toString());
        }
    }
}
