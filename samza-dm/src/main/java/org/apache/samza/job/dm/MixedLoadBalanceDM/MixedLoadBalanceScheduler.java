package org.apache.samza.job.dm.MixedLoadBalanceDM;

        import org.apache.hadoop.yarn.api.records.Resource;
        import org.apache.samza.config.Config;
        import org.apache.samza.config.DMSchedulerConfig;
        import org.apache.samza.job.dm.*;
        import org.apache.samza.job.model.JobModel;
        import org.apache.samza.job.dm.MixedLoadBalancer.MixedLoadBalanceManager;

        import java.util.concurrent.ConcurrentMap;
        import java.util.concurrent.ConcurrentSkipListMap;
        import java.util.logging.Logger;

public class MixedLoadBalanceScheduler implements DMScheduler {
    MixedLoadBalanceManager balanceManager;
    //private static final Logger LOG = Logger.getLogger(DefaultScheduler.class.getName());

    private Config config;
    private DMSchedulerConfig schedulerConfig;

    private ConcurrentMap<String, Stage> stages;

    private MixedLoadBalanceDispatcher dispatcher;

    private DMSchedulingPolicy policy = new DefaultSchedulingPolicy();

    private int prev = -1;
    private boolean scaled = false;
    private long prevTime = -1;

    @Override
    public void init(Config config, DMSchedulerConfig schedulerConfig) {
        this.config = config;
        this.schedulerConfig = schedulerConfig;
        this.stages = new ConcurrentSkipListMap<>();

        this.dispatcher = new MixedLoadBalanceDispatcher();
        this.dispatcher.init(config);

        balanceManager = new MixedLoadBalanceManager();
        balanceManager.initial(config);
    }

    @Override
    public Allocation allocate(Resource clusterResource) {
        return null;
    }

    @Override
    public Allocation getDefaultAllocation(String stageId) {
        return new Allocation(stageId);
    }

    public Allocation getDefaultAllocation(String stageId, String parallelism) {
        return new Allocation(stageId, Integer.valueOf(parallelism));
    }

    @Override
    public void createListener(DMScheduler scheduler) {
        writeLog("starting listener in scheduler");
        MixedLoadBalanceSchedulerListener listener = new MixedLoadBalanceSchedulerListener();
        listener.setScheduler(this);
        listener.setConfig(config);
        listener.startListener();
    }

    @Override
    public void submitApplication() {
        writeLog("scheduler submit application");
        // Use default schema to launch the application
        Allocation defaultAllocation = getDefaultAllocation(config.get("job.name"), config.get("job.container.count","1"));
        dispatcher.submitApplication(defaultAllocation);

        // for kafka listener
        createListener(this);
        while (true) {}
    }

    @Override
    public DMDispatcher getDispatcher(String DMDispatcherClass) {
        writeLog("scheduler getdispatcher");
        DMDispatcher dispatcher = null;
        try {
            dispatcher = (DMDispatcher) Class.forName(DMDispatcherClass).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return dispatcher;
    }

    @Override
    public void dispatch(Allocation allocation) {
    }

    @Override
    public void updateStage(StageReport report) {
        /*if (report.getType().equals("ApplicationMaster")) {
            writeLog("update application master ip address");
            if (!stages.containsKey(report.getName())) {
                writeLog("creating new stage for application master");
                stages.put(report.getName(), new Stage());
            }
            Stage curr = stages.get(report.getName());
            if (report.getRunningContainers() != 0) curr.setRunningContainers(report.getRunningContainers());
            stages.put(report.getName(), curr);
            this.dispatcher.updateEnforcerURL(report.getName(), report.getHost()+ ":1999");
        } else if (report.getType().contains("TaskName-Partition")) {

            long timeDiff = prevTime == -1 ? prev : report.getTime() - prevTime;
            if (timeDiff > 100) {
                Stage curr = stages.get(report.getName());

                int temp = report.getThroughput();
                if (prev != -1) {
                    report.setThroughput((report.getThroughput() - prev) / 5);
                }

                writeLog("Throughput:" + report.getThroughput() + "  " + "runningcontainers: " + curr.getRunningContainers());

                prev = temp;
                Allocation result = this.policy.allocate(curr, report);
                if (result.getParallelism() != 0 && !scaled) {
                    this.dispatcher.enforceSchema(result);
                    this.scaled = true;
                }
            }

            prevTime = report.getTime();
        }*/
    }
    public void updateJobModel(){
        if(!balanceManager.checkLoad()){
            //Rebalance the JobModel
            JobModel newJobModel = balanceManager.rebalanceJobModel();
            if(newJobModel == null){
                //need to scale
                System.out.printf("Need to scale up");
                newJobModel = balanceManager.scaleUpByNumber(1);
                dispatcher.changeParallelism(getDefaultAllocation(config.get("job.name")), newJobModel.getContainers().size(), newJobModel);
            }else {
                System.out.printf("New Job Model is:\n" + newJobModel.toString());
                //Dispatch the new JobModel
                dispatcher.updateJobModel(getDefaultAllocation(config.get("job.name")), newJobModel);
            }
        }
    }

    // Update leader's address from kafka metric topic
    public boolean updateLeader(StageReport report){
        if (report.getType().equals("ApplicationMaster")) {
            writeLog("update application master ip address");
            if (!stages.containsKey(report.getName())) {
                writeLog("creating new stage for application master");
                stages.put(report.getName(), new Stage());
            }
            Stage curr = stages.get(report.getName());
            if (report.getRunningContainers() != 0) curr.setRunningContainers(report.getRunningContainers());
            stages.put(report.getName(), curr);
            this.dispatcher.updateEnforcerURL(report.getName(), report.getHost()+ ":1999");
            return true;
        }
        return false;
    }
    private void writeLog(String log){
        System.out.println("MixedLoadBalanceScheduler: " + log);
    }
}
