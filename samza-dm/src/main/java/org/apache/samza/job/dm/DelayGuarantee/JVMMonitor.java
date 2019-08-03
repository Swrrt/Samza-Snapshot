package org.apache.samza.job.dm.DelayGuarantee;

import org.apache.samza.zk.RMI.UtilizationClient;
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
    private String leaderAddr = "";
    private String processorId = "";
    private UtilizationClient client;
    JVMMonitor(){
        peOperatingSystemMXBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    }
    private float getJvmPhyMemoryUsage(){
        return peOperatingSystemMXBean.getTotalPhysicalMemorySize() - peOperatingSystemMXBean.getFreePhysicalMemorySize();
    }
    private float getJvmTotalMemoryUsage(){
        return peOperatingSystemMXBean.getTotalPhysicalMemorySize() + peOperatingSystemMXBean.getTotalSwapSpaceSize() - peOperatingSystemMXBean.getFreePhysicalMemorySize() - peOperatingSystemMXBean.getFreeSwapSpaceSize();
    }
    private float getJvmCpuUsage() {
        // elapsed process time is in nanoseconds
        long prevElapsedProcessCpuTime = peOperatingSystemMXBean.getProcessCpuTime();
        // elapsed uptime is in milliseconds
        long prevElapsedJvmUptime = runtimeMXBean.getUptime();
        // the available processors
        // TODO: replace this with number of cores YARN gives;
        int availableProcessors = 1;//operatingSystemMXBean.getAvailableProcessors();
        try{
            Thread.sleep(300);
        }catch (Exception e){
        }
        long elapsedProcessCpuTime = peOperatingSystemMXBean.getProcessCpuTime() - prevElapsedProcessCpuTime;
        // elapsed uptime is in milliseconds
        long elapsedJvmUptime = runtimeMXBean.getUptime() - prevElapsedJvmUptime;

        // calculate cpu usage as a percentage value
        // to convert nanoseconds to milliseconds divide it by 1000000 and to get a percentage multiply it by 100
        float cpuUsage = elapsedProcessCpuTime / (elapsedJvmUptime * 10000F * availableProcessors);
        //LOG.info("Elapsed CPU Time:" + elapsedProcessCpuTime + "  |  ElapsedJvmUpTime:"+ elapsedJvmUptime +  "  |  AvailableProcessors:"+operatingSystemMXBean.getAvailableProcessors());

        // set old timestamp values
        return cpuUsage;
    }
    public void run(){
        LOG.info("Running JVM CPU and memory monitor");
        client = new UtilizationClient(leaderAddr, 8883);
        try{
            while(true){
                Float i = getJvmCpuUsage(), j = getJvmPhyMemoryUsage(), k = getJvmTotalMemoryUsage();
                LOG.info("JVM CPU usage is: "+i.toString());
                LOG.info("JVM physical memory usage is: " + j.toString() + "  ; Total memory usage is: " + k.toString());
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
