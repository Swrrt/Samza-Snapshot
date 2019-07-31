package org.apache.samza.job.dm.MixedLoadBalanceDM;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.samza.config.Config;
import org.apache.samza.job.dm.MixedLoadBalancer.MixedLoadBalanceManager;
import org.apache.samza.scheduler.LoadScheduler;
import org.apache.samza.scheduler.LoadSchedulerRunloop;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class MixedLoadBalanceSchedulerRunloop implements LoadSchedulerRunloop {
    MixedLoadBalanceScheduler scheduler;
    Config config;
    MixedLoadBalanceManager loadBalanceManager;
    boolean leaderComes;
    @Override
    public void start() {
        writeLog("Start load-balancer scheduler listener");

        String metricsTopicName = config.get("metrics.reporter.snapshot.stream", "kafka.metrics").substring(6);
//        String metricsTopicName = "metrics";

        leaderComes = false;
        long lastTime = System.currentTimeMillis(), rebalanceInterval = config.getInt("job.loadbalance.interval", 1000);
        long retrieveInterval = config.getInt("job.loadbalance.delay.interval", 500);
        long startTime = -1; // The time AM is coming.
        long warmupTime = config.getLong("job.loadbalance.warmup.time", 30000);
        long migrationTimes = 0;
        while (true) {
            //writeLog("Try to retrieve report");
            try{
                Thread.sleep(retrieveInterval);
            }catch (Exception e){};

            long time = System.currentTimeMillis() ;
            if(!leaderComes && scheduler.updateLeader()){
                leaderComes = true;
            }
            /*
            */

            if(loadBalanceManager.retrieveArrivedAndProcessed(time))lastTime = time;
            loadBalanceManager.updateDelay(time);
            loadBalanceManager.updateModelingData(time);

            //Try to rebalance periodically
            if(leaderComes && time - startTime > warmupTime) {
                long nowTime = System.currentTimeMillis();
                if(nowTime - lastTime >= rebalanceInterval) {
                    migrationTimes ++;
                    //loadBalanceManager.showDelayMetrics("before" + migrationTimes);
                    writeLog("Try to rebalance");
                    if(scheduler.tryToRebalance()) lastTime = nowTime;
                    //loadBalanceManager.showDelayMetrics("migrated" + migrationTimes);
                    //lastTime = nowTime;
                }else{
                    //writeLog("Smaller than rebalanceInterval, wait for next loop");
                }
            }
        }
    }

    @Override
    public void setScheduler(LoadScheduler scheduler) {
        this.scheduler = (MixedLoadBalanceScheduler)scheduler;
        this.loadBalanceManager = this.scheduler.getBalanceManager();
    }
    @Override
    public void setConfig(Config config) {
        this.config = config;
    }
    private void writeLog(String log){
        System.out.println(log);
    }
}
