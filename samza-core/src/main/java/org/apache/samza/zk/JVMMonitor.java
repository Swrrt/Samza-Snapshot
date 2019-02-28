package org.apache.samza.zk;

import org.apache.samza.RMI.UtilizationClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class JVMMonitor implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(JVMMonitor.class);
    private Thread t;
    private final int MonitorSleepInterval = 3000;
    private com.sun.management.OperatingSystemMXBean peOperatingSystemMXBean;
    private java.lang.management.OperatingSystemMXBean operatingSystemMXBean;
    private RuntimeMXBean runtimeMXBean;
    private long previousJvmProcessCpuTime = 0;
    private long previousJvmUptime = 0;
    private String leaderAddr = "";
    private String processorId = "";
    private UtilizationClient client;
    JVMMonitor(){
        peOperatingSystemMXBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    }
    private float getJvmCpuUsage() {
        // elapsed process time is in nanoseconds
        long elapsedProcessCpuTime = peOperatingSystemMXBean.getProcessCpuTime() - previousJvmProcessCpuTime;
        // elapsed uptime is in milliseconds
        long elapsedJvmUptime = runtimeMXBean.getUptime() - previousJvmUptime;

        // total jvm uptime on all the available processors
        long totalElapsedJvmUptime = elapsedJvmUptime * operatingSystemMXBean.getAvailableProcessors();

        // calculate cpu usage as a percentage value
        // to convert nanoseconds to milliseconds divide it by 1000000 and to get a percentage multiply it by 100
        float cpuUsage = elapsedProcessCpuTime / (elapsedJvmUptime * 10000F);
        //LOG.info("Elapsed CPU Time:" + elapsedProcessCpuTime + "  |  ElapsedJvmUpTime:"+ elapsedJvmUptime +  "  |  AvailableProcessors:"+operatingSystemMXBean.getAvailableProcessors());

        // set old timestamp values
        previousJvmProcessCpuTime = peOperatingSystemMXBean.getProcessCpuTime();
        previousJvmUptime = runtimeMXBean.getUptime();

        return cpuUsage;
    }
    public void run(){
        LOG.info("Running JVM CPU and memory monitor");
        client = new UtilizationClient(leaderAddr, 8883);
        try{
            while(true){
                Float i = getJvmCpuUsage();
                LOG.info("JVM CPU usage is: "+i.toString());
                //Currently only send the Utilization information when workload is too heavy or too light
                if(i<50.0||i>80.0) client.sendUtilization(processorId, i);
                Thread.sleep(MonitorSleepInterval);
            }
        }catch(Exception e){
            LOG.info("Exception happens: "+e.toString());
        }
        LOG.info("JVM CPU monitor stopped");
    }
    public void start(String leaderAddr, String processorId){
        LOG.info("Starting JVM monitor with Leader Address:" +leaderAddr);
        this.leaderAddr = leaderAddr;
        this.processorId = processorId;
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
