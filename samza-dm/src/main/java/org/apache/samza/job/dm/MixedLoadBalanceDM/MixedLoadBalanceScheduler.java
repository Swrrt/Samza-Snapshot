package org.apache.samza.job.dm.MixedLoadBalanceDM;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.DMSchedulerConfig;
import org.apache.samza.job.dm.*;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.dm.MixedLoadBalancer.MixedLoadBalanceManager;
import org.apache.samza.scheduler.LoadScheduler;
import org.json.JSONObject;

public class MixedLoadBalanceScheduler implements LoadScheduler {
    private MixedLoadBalanceManager balanceManager;
    //private static final Logger LOG = Logger.getLogger(DefaultScheduler.class.getName());

    private Config config;

    private MixedLoadBalanceDispatcher dispatcher;
    private RMIMetricsRetriever metricsRetriever;

    public void createAndStartRunloop(LoadScheduler scheduler) {
        writeLog("starting listener in scheduler");
        MixedLoadBalanceSchedulerRunloop runloop = new MixedLoadBalanceSchedulerRunloop();
        runloop.setScheduler(scheduler);
        runloop.setConfig(config);
        runloop.start();
    }
    @Override
    public void init(Config config){
        this.config = config;

        this.dispatcher = new MixedLoadBalanceDispatcher();
        this.dispatcher.init();

        this.metricsRetriever = new RMIMetricsRetriever();

        this.balanceManager = new MixedLoadBalanceManager();
        this.balanceManager.initial(config, this.metricsRetriever);

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
        if(!balanceManager.checkMigrationDeployed()){
            writeLog("Last migration is not deployed, cannot rebalance");
            return false;
        }
        if (!balanceManager.checkDelay()) {
            //Rebalance the JobModel
            RebalanceResult rebalanceResult = balanceManager.migratingOnce(); //randomMoveOneTask(time);//balanceManager.rebalanceJobModel();
            JobModel newJobModel = null;
            if (rebalanceResult.getCode() == RebalanceResult.RebalanceResultCode.Migrating) {
                //balanceManager.updateTaskContainers(rebalanceResult.getTaskContainer());
                newJobModel = balanceManager.generateJobModel(rebalanceResult.getTaskContainer());
                writeLog("New Job Model is:" + newJobModel.toString() + ", prepare to dispatch");
                JobModelDemonstrator.demoJobModel(newJobModel);
                balanceManager.stashNewJobModel(newJobModel);
                balanceManager.stashNewRebalanceResult(rebalanceResult);
                balanceManager.updateMigrationContext(rebalanceResult.getMigrationContext());
                //balanceManager.updateOldJobModel(newJobModel);
                //Dispatch the new JobModel
                dispatcher.updateJobModel(config.get("job.name"), newJobModel);
                return true;
            } else if (rebalanceResult.getCode() == RebalanceResult.RebalanceResultCode.NeedScalingOut) {
                writeLog("Need to scale out");
                rebalanceResult = balanceManager.scaleOutByNumber(1);
                if (rebalanceResult.getCode() != RebalanceResult.RebalanceResultCode.ScalingOut) {
                    writeLog("Something is wrong when try to scale out");
                    return false;
                }
                //balanceManager.updateTaskContainers(rebalanceResult.getTaskContainer());
                newJobModel = balanceManager.generateJobModel(rebalanceResult.getTaskContainer());
                writeLog("New Job Model is:" + newJobModel.toString() + ", prepare to dispatch");
                JobModelDemonstrator.demoJobModel(newJobModel);
                balanceManager.stashNewJobModel(newJobModel);
                balanceManager.stashNewRebalanceResult(rebalanceResult);
                balanceManager.updateMigrationContext(rebalanceResult.getMigrationContext());
                dispatcher.changeParallelism(config.get("job.name"), newJobModel.getContainers().size(), newJobModel);
                return true;
            }
        }else if(balanceManager.getOldJobModel().getContainers().size() > 1){   ////Try to scale in
            writeLog("No need to rebalance, try to scale in");
            RebalanceResult rebalanceResult = balanceManager.scaleInByOne();
            if (rebalanceResult.getCode() == RebalanceResult.RebalanceResultCode.ScalingIn) {
                writeLog("Need to Scale In");
                JobModel newJobModel = balanceManager.generateJobModel(rebalanceResult.getTaskContainer());
                writeLog("New Job Model is:" + newJobModel.toString() + ", prepare to dispatch");
                JobModelDemonstrator.demoJobModel(newJobModel);
                balanceManager.stashNewJobModel(newJobModel);
                balanceManager.stashNewRebalanceResult(rebalanceResult);
                balanceManager.updateMigrationContext(rebalanceResult.getMigrationContext());
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

    public MixedLoadBalanceManager getBalanceManager() {
        return balanceManager;
    }

    private void writeLog(String log) {
        System.out.println("MixedLoadBalanceScheduler: " + log);
    }
}
