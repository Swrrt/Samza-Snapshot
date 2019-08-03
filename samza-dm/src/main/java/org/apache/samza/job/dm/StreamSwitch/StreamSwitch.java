package org.apache.samza.job.dm.StreamSwitch;

import org.apache.samza.config.Config;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.dm.DelayGuarantee.DelayGuaranteeDecisionModel;
import org.apache.samza.scheduler.LoadScheduler;

public class StreamSwitch implements LoadScheduler {
    private DelayGuaranteeDecisionModel decisionModel;
    //private static final Logger LOG = Logger.getLogger(DefaultScheduler.class.getName());

    private Config config;

    private LeaderDispatcher dispatcher;
    private RMIMetricsRetriever metricsRetriever;

    public void createAndStartRunloop(LoadScheduler scheduler) {
        writeLog("Scheduler's runloop starting");

        boolean leaderComes = false;
        long lastTime = System.currentTimeMillis(), rebalanceInterval = config.getInt("job.loadbalance.interval", 1000);
        long retrieveInterval = config.getInt("job.loadbalance.delay.interval", 500);
        long startTime = -1; // The time AM is coming.
        long warmupTime = config.getLong("job.loadbalance.warmup.time", 30000);
        long migrationTimes = 0;
        while (true) {
            //writeLog("Try to retrieve report");

            long time = System.currentTimeMillis() ;
            if(!leaderComes && updateLeader()){
                leaderComes = true;
                startTime = time;
            }

            if(decisionModel.retrieveArrivedAndProcessed(time))lastTime = time;
            decisionModel.updateDelay(time);
            decisionModel.updateModelingData(time);

            //Try to rebalance periodically
            if(leaderComes && time - startTime > warmupTime) {
                long nowTime = System.currentTimeMillis();
                if(nowTime - lastTime >= rebalanceInterval) {
                    migrationTimes ++;
                    //loadBalanceManager.showDelayMetrics("before" + migrationTimes);
                    writeLog("Try to rebalance");
                    if(tryToRebalance()) lastTime = nowTime;
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
        this.decisionModel.initial(config, this.metricsRetriever);

        this.metricsRetriever.start();
    }

    //Running call from LoadbalanceScheduler
    @Override
    public void start(){
        writeLog("Starting Scheduler");
        createAndStartRunloop(this);
        while(true){
        }
    }

    //Return true if change the jobModel
    public boolean tryToRebalance() {
        if(!decisionModel.checkMigrationDeployed()){
            writeLog("Last migration is not deployed, cannot rebalance");
            return false;
        }
        if (!decisionModel.checkDelay()) {
            //Rebalance the JobModel
            RebalanceResult rebalanceResult = decisionModel.migratingOnce(); //randomMoveOneTask(time);//decisionModel.rebalanceJobModel();
            JobModel newJobModel = null;
            if (rebalanceResult.getCode() == RebalanceResult.RebalanceResultCode.Migrating) {
                //decisionModel.updateTaskContainers(rebalanceResult.getTaskContainer());
                newJobModel = decisionModel.generateJobModel(rebalanceResult.getTaskContainer());
                writeLog("New Job Model is:" + newJobModel.toString() + ", prepare to dispatch");
                JobModelDemonstrator.demoJobModel(newJobModel);
                decisionModel.stashNewJobModel(newJobModel);
                decisionModel.stashNewRebalanceResult(rebalanceResult);
                decisionModel.updateMigrationContext(rebalanceResult.getMigrationContext());
                //decisionModel.updateOldJobModel(newJobModel);
                //Dispatch the new JobModel
                dispatcher.updateJobModel(config.get("job.name"), newJobModel);
                return true;
            } else if (rebalanceResult.getCode() == RebalanceResult.RebalanceResultCode.NeedScalingOut) {
                writeLog("Need to scale out");
                rebalanceResult = decisionModel.scaleOutByNumber(1);
                if (rebalanceResult.getCode() != RebalanceResult.RebalanceResultCode.ScalingOut) {
                    writeLog("Something is wrong when try to scale out");
                    return false;
                }
                //decisionModel.updateTaskContainers(rebalanceResult.getTaskContainer());
                newJobModel = decisionModel.generateJobModel(rebalanceResult.getTaskContainer());
                writeLog("New Job Model is:" + newJobModel.toString() + ", prepare to dispatch");
                JobModelDemonstrator.demoJobModel(newJobModel);
                decisionModel.stashNewJobModel(newJobModel);
                decisionModel.stashNewRebalanceResult(rebalanceResult);
                decisionModel.updateMigrationContext(rebalanceResult.getMigrationContext());
                dispatcher.changeParallelism(config.get("job.name"), newJobModel.getContainers().size(), newJobModel);
                return true;
            }
        }else if(decisionModel.getOldJobModel().getContainers().size() > 1){   ////Try to scale in
            writeLog("No need to rebalance, try to scale in");
            RebalanceResult rebalanceResult = decisionModel.scaleInByOne();
            if (rebalanceResult.getCode() == RebalanceResult.RebalanceResultCode.ScalingIn) {
                writeLog("Need to Scale In");
                JobModel newJobModel = decisionModel.generateJobModel(rebalanceResult.getTaskContainer());
                writeLog("New Job Model is:" + newJobModel.toString() + ", prepare to dispatch");
                JobModelDemonstrator.demoJobModel(newJobModel);
                decisionModel.stashNewJobModel(newJobModel);
                decisionModel.stashNewRebalanceResult(rebalanceResult);
                decisionModel.updateMigrationContext(rebalanceResult.getMigrationContext());
                dispatcher.changeParallelism(config.get("job.name"), newJobModel.getContainers().size(), newJobModel);
                return true;
            }
            writeLog("Cannot scale in");
        }
        return false;
    }

    // Update leader's address from kafka metric topic
    public boolean updateLeader() {
        String leaderAddress = metricsRetriever.getLeaderAddress();
        if(leaderAddress != null){
                //writeLog("New application master ip: " + host);
            this.dispatcher.updateEnforcerURL(config.get("job.name"), leaderAddress + ":1999");
            return true;
        }
        return false;
    }

    public DelayGuaranteeDecisionModel getBalanceManager() {
        return decisionModel;
    }

    private void writeLog(String log) {
        System.out.println("DelayGuaranteeScheduler: " + log);
    }
}
