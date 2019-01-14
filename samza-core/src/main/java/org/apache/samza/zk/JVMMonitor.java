package org.apache.samza.zk;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JVMMonitor implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(JVMMonitor.class);
    private JVMCPUUsage jvmcpuUsage;
    private Thread t;
    private final int MonitorSleepInterval = 1000;
    JVMMonitor(){
        jvmcpuUsage = new JVMCPUUsage();
    }
    public void run(){
        LOG.info("Running JVM CPU and memory monitor");

        try{
            jvmcpuUsage.openMBeanServerConnection();
            jvmcpuUsage.getMXBeanProxyConnections();
        }catch(Exception e){
            LOG.info("Exception when connecting to MBean server: "+e.toString());
        }
        try{
            while(true){
                Float i = jvmcpuUsage.getJvmCpuUsage();
                LOG.info("JVM CPU usage is: "+i.toString());
                Thread.sleep(MonitorSleepInterval);
            }
        }catch(Exception e){
            LOG.info("Exception happens: "+e.toString());
        }
        LOG.info("JVM CPU monitor stopped");
    }
    public void start(){
        LOG.info("Starting JVM monitor");
        if(t == null){
            t = new Thread(this, "JVM monitor");
            t.start();
        }
    }
}
