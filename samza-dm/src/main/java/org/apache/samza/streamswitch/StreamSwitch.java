package org.apache.samza.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.dispatcher.LeaderDispatcher;
import org.apache.samza.metrics.RMIMetricsRetriever;
import org.apache.samza.models.delayguarantee.DelayGuaranteeDecisionModel;
import org.apache.samza.scheduler.LoadScheduler;

public class StreamSwitch implements LoadScheduler {
    private DelayGuaranteeDecisionModel decisionModel;
    //private static final Logger LOG = Logger.getLogger(DefaultScheduler.class.getName());

    private Config config;

    private LeaderDispatcher dispatcher;
    private RMIMetricsRetriever metricsRetriever;

    public void startRunloop() {
        writeLog("Scheduler's runloop starting");

        long lastTime = System.currentTimeMillis(), rebalanceInterval = config.getInt("job.loadbalance.interval", 1000);
        long retrieveInterval = config.getInt("job.loadbalance.delay.interval", 500);
        long startTime = -1; // The time Application starts running. -1 for not start yet.
        long warmupTime = config.getLong("job.loadbalance.warmup.time", 30000);
        long migrationTimes = 0;
        while (true) {
            //writeLog("Try to retrieve report");

            long time = System.currentTimeMillis() ;
            if(startTime == - 1 && metricsRetriever.isApplicationRunning()){
                startTime = time;
            }

            /*if(!leaderComes && updateLeader()){
                leaderComes = true;
                startTime = time;
            }*/

            if(decisionModel.retrieveMetrics(time))lastTime = time;
            decisionModel.updateModel(time);


            //Try to rebalance periodically
            if(dispatcher.okToDispatch() && time - startTime > warmupTime) {
                long nowTime = System.currentTimeMillis();
                if(nowTime - lastTime >= rebalanceInterval) {
                    migrationTimes ++;
                    //loadBalanceManager.showDelayMetrics("before" + migrationTimes);
                    writeLog("Try to rebalance");
                    if(decisionModel.tryToRebalance()) lastTime = nowTime;
                    //loadBalanceManager.showDelayMetrics("migrated" + migrationTimes);
                    //lastTime = nowTime;
                }else{
                    //writeLog("Smaller than rebalanceInterval, wait for next loop");
                }
            }
            try{
                Thread.sleep(retrieveInterval);
            }catch (Exception e){};
        }
    }
    @Override
    public void init(Config config){
        this.config = config;

        this.dispatcher = new LeaderDispatcher();
        this.dispatcher.init();

        this.metricsRetriever = new RMIMetricsRetriever();

        this.decisionModel = new DelayGuaranteeDecisionModel();
        this.decisionModel.initial(config, this.metricsRetriever, this.dispatcher);
        this.metricsRetriever.setDispatcher(dispatcher);
        this.metricsRetriever.start();
    }

    //Running call from LoadbalanceScheduler
    @Override
    public void start(){
        writeLog("Starting Scheduler");
        startRunloop();
        while(true){
        }
    }

    private void writeLog(String log) {
        System.out.println("DelayGuaranteeScheduler: " + log);
    }
}
