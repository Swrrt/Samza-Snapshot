package org.apache.samza.job.dm.MixedLoadBalanceDM;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.samza.config.Config;
import org.apache.samza.job.dm.DMScheduler;
import org.apache.samza.job.dm.DMSchedulerListener;
import org.apache.samza.job.dm.MixedLoadBalancer.MixedLoadBalanceManager;
import org.apache.samza.job.dm.StageReport;

import java.util.Arrays;
import java.util.Properties;

public class MixedLoadBalanceSchedulerListener implements DMSchedulerListener {
    MixedLoadBalanceScheduler scheduler;
    Config config;
    MixedLoadBalanceManager loadBalanceManager;
    boolean leaderComes;
    @Override
    public void startListener() {
        writeLog("Start load-balancer scheduler listener");

        String metricsTopicName = config.get("metrics.reporter.snapshot.stream", "kafka.metrics").substring(6);
//        String metricsTopicName = "metrics";
        Properties props = new Properties();
        props.put("bootstrap.servers", config.get("systems.kafka.producer.bootstrap.servers"));
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer <String, String>(props);

        consumer.subscribe(Arrays.asList(metricsTopicName));

        writeLog("Subscribing to kafka metrics stream: " + metricsTopicName);
        leaderComes = false;
        long lastTime = System.currentTimeMillis(), rebalanceInterval = config.getInt("job.loadbalance.interval", 20000);
        while (true) {
            try{
                Thread.sleep(5000);
            }catch (Exception e){
            }
            writeLog("Try to retrieve report");
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {

               // writeLog("Reports: " + record.toString());
                if (scheduler.updateLeader(record)) {
                    leaderComes = true;
                }
            }

            //Try to rebalance periodically
            if(leaderComes) {
                long nowTime = System.currentTimeMillis();
                if(nowTime - lastTime >= rebalanceInterval) {
                    writeLog("Try to rebalance");
                    scheduler.updateJobModel();
                }else{
                    writeLog("Smaller than rebalanceInterval, wait for next loop");
                }
            }
        }
    }

    @Override
    public void setScheduler(DMScheduler scheduler) {
        this.scheduler = (MixedLoadBalanceScheduler)scheduler;
        this.loadBalanceManager = this.scheduler.balanceManager;
    }
    @Override
    public void setConfig(Config config) {
        this.config = config;
    }
    private void writeLog(String log){
        System.out.println(log);
    }
}
