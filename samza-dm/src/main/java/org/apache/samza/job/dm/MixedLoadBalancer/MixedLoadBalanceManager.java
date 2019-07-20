package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.dm.MixedLoadBalanceDM.*;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.apache.samza.config.Config;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;

//Need to bind
public class MixedLoadBalanceManager {
    //private static final Logger LOG = LoggerFactory.getLogger(MixedLoadBalanceManager.class);
    private final int LOCALITY_RETRY_TIMES = 2;
    private double instantaneousThreshold;
    private double longTermThreshold;
    //TODO: reorganize all classes and tables;
    //private ConsistentHashing consistentHashing;
    //private LocalityDistance locality;  //TODO: update Locality part.
    private WebReader webReader;
    //private Map<String,List<String>> hostRack = null;
    //private Map<String,String> containerHost = null;
    private Map<String, String> taskContainer = null;
    private Set<String> containerIds = null;
    private AtomicInteger nextContainerId;
    private Map<Integer, String> partitionTask = null;
    private Map<String, TaskModel> tasks; //Existing tasks
    private JobModel oldJobModel;
    // Metrics
    //private Map<String, ContainerMigratingState> containerMigratingState = null;
    private MigrationContext migrationContext = null;
    private ModelingData modelingData;
    private Config config;
    //private LocalityServer localityServer = null;
    //private OffsetServer offsetServer = null;
    private RMIMetricsRetriever metricsListener = null;
    private SnapshotMetricsRetriever snapshotMetricsRetriever = null;
    private DelayEstimator delayEstimator = null;
    public MixedLoadBalanceManager(){
        config = null;
        //consistentHashing = new ConsistentHashing();
        //locality = new LocalityDistance();
        webReader = new WebReader();
        partitionTask = new HashMap<>();
        taskContainer = new HashMap<>();
        containerIds = new HashSet<>();
        //containerHost = new HashMap<>();
        //hostRack = new HashMap<>();
        tasks = new HashMap<>();
        oldJobModel = null;
        //defaultVNs = 10;
        //utilizationServer = new UtilizationServer();
        //unprocessedMessageMonitor = new UnprocessedMessageMonitor();
        //localityServer = new LocalityServer();
        //offsetServer = new OffsetServer();
        metricsListener = new RMIMetricsRetriever();
        //kafkaOffsetRetriever = new KafkaOffsetRetriever();
        snapshotMetricsRetriever = new SnapshotMetricsRetriever();
        delayEstimator = new DelayEstimator();
        //containerMigratingState = new ConcurrentHashMap<>();
        modelingData = new ModelingData();
        migrationContext = new MigrationContext();
        nextContainerId = new AtomicInteger();
    }
    /*
        TODO:
        Generate initial JobModel in here.
     */
    public void initial(Config config){
        //Generate initial job model according to
        Config coordinatorSystemConfig = Util.buildCoordinatorStreamConfig(config);
        JobModelManager jobModelManager = JobModelManager.apply(coordinatorSystemConfig, new MetricsRegistryMap());
        oldJobModel = jobModelManager.jobModel();
        migrationContext.setDeployed();
        initial(oldJobModel, config);
    }
    public int getNextContainerId(){
        return nextContainerId.get();
    }
    public void setNextContainerId(int id){
        if(id > nextContainerId.get()) {
            nextContainerId.set(id);
        }
    }
    /*
        initial
     */
    public void initial(JobModel jobModel, Config config){
        writeLog("MixedLoadBalanceManager is initializing");
       // getHostRack();
        this.config = config;
        /*
            We need group id to read offset information.
            Group id is generated randomly in SamzaContainer(KafkaSystemFactory.getConsumer())
            So we need a way to catch them.
         */

        snapshotMetricsRetriever.initial(config.get("job.name"), config.get("job.loadbalance.inputtopic"));
        snapshotMetricsRetriever.setOffsetClient(config);
        //kafkaOffsetRetriever.initial(config.subset("system.kafka"),config.get("job.loadbalance.inputtopic")); //TODO: need input topic name

        oldJobModel = jobModel;
        setNextContainerId(oldJobModel.getContainers().size() + 2);
        updateFromJobModel(jobModel);
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            for(Map.Entry<TaskName, TaskModel> taskModel: containerModel.getTasks().entrySet()){
                //Create new tasks model!
                tasks.put(taskModel.getKey().getTaskName(), new TaskModel(taskModel.getValue().getTaskName(),taskModel.getValue().getSystemStreamPartitions(),taskModel.getValue().getChangelogPartition()));
            }
        }
        //writeLog("Task Models:" + tasks.toString());
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            //insertContainer(containerModel.getProcessorId());
        }
        modelingData.setDelayEstimator(delayEstimator);
        modelingData.setTimes(delayEstimator.timePoints);
        modelingData.setTimes(config.getLong("job,loadbalance.delay.interval", 500l), config.getInt("job.loadbalance.delay.alpha", 20), config.getInt("job.loadbalance.delay.beta", 10));
        //unprocessedMessageMonitor.init(config.get("systems.kafka.producer.bootstrap.servers"), "metrics", config.get("job.name"));
        instantaneousThreshold = config.getDouble("job.loadbalance.delay.instant.threshold", 100.0);
        longTermThreshold = config.getDouble("job.loadbalance.delay.longterm.threshold", 100.0);
        metricsListener.setJobModelVersions(containerIds);
        metricsListener.start();
        /*for(String containerId: containerIds){
            containerJobModelVersion.put(containerId, -1l);
        }*/
        //unprocessedMessageMonitor.start();
        //utilizationServer.start();
        //localityServer.start();
        //offsetServer.start();
    }

    private Map<String, String> getTaskContainer(JobModel jobModel){
        Map<String, ContainerModel> containers = jobModel.getContainers();
        Map<String, String> taskContainer = new HashMap<>();
        for(ContainerModel container: containers.values()){
            for(TaskModel task: container.getTasks().values()){
                taskContainer.put(task.getTaskName().getTaskName(), container.getProcessorId());
            }
        }
        return taskContainer;
    }

    public boolean checkMigrationDeployed(){
        if(migrationContext == null)return false;
        return migrationContext.isDeployed();
    }
    private JobModel newJobModel;
    public void stashNewJobModel(JobModel jobModel){
        newJobModel = jobModel;
    }
    private RebalanceResult newRebalanceResult;
    public void stashNewRebalanceResult(RebalanceResult rebalanceResult){
        newRebalanceResult = rebalanceResult;
    }
    public void updateMigrationContext(MigrationContext newMigrationContext){
        migrationContext = newMigrationContext;
    }
    // Generate JobModel according to taskContainer
    public JobModel generateJobModel(Map<String, String> taskContainer){
        //generate new job model from current containers and tasks setting
        //store the new job model for future use;
        Set<String> containerIds = new HashSet<>();
        writeLog("Generating new job model...");
        containerIds.addAll(taskContainer.values());
        writeLog("Containers: "+ containerIds);
        //writeLog("Task-Containers: " + taskContainer);
        //writeLog("Tasks: "+ taskContainer.keySet());
        Map<String, LinkedList<TaskModel>> containerTasks = new HashMap<>();
        Map<String, ContainerModel> containers = new HashMap<>();
        for(String container: containerIds){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            containerTasks.put(processor, new LinkedList<>());
        }
        //Add taskModel according to taskContainer
        for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            String containerId = taskContainer.get(task.getKey());
            //    writeLog("containerId: " + containerId);
            containerTasks.get(containerId).add(task.getValue());
        }

        for(String container: containerIds){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            Map<TaskName, TaskModel> tasks = new HashMap<>();
            for(TaskModel task: containerTasks.get(container)){
                tasks.put(task.getTaskName(),task);
            }
            containers.put(processor, new ContainerModel(processor,0, tasks));
        }
        JobModel jobModel = new JobModel(config, containers);
        JobModelDemonstrator.demoJobModel(jobModel);
        //writeLog("New job model:" + oldJobModel.toString());*/
        return jobModel;
    }

    public JobModel generateJobModel(){
        //generate new job model from current containers and tasks setting
        //store the new job model for future use;
        writeLog("Generating new job model...");
        containerIds.addAll(taskContainer.values());
        writeLog("Containers: "+ containerIds);
        //writeLog("Task-Containers: " + taskContainer);
        //writeLog("Tasks: "+ taskContainer.keySet());
        Map<String, LinkedList<TaskModel>> containerTasks = new HashMap<>();
        Map<String, ContainerModel> containers = new HashMap<>();
        for(String container: containerIds){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            containerTasks.put(processor, new LinkedList<>());
        }
        //Add taskModel according to taskContainer
        for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            String containerId = taskContainer.get(task.getKey());
        //    writeLog("containerId: " + containerId);
            containerTasks.get(containerId).add(task.getValue());
        }

        for(String container: containerIds){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            Map<TaskName, TaskModel> tasks = new HashMap<>();
            for(TaskModel task: containerTasks.get(container)){
                tasks.put(task.getTaskName(),task);
            }
            containers.put(processor, new ContainerModel(processor,0, tasks));
        }
        oldJobModel = new JobModel(config, containers);
        JobModelDemonstrator.demoJobModel(oldJobModel);
        //writeLog("New job model:" + oldJobModel.toString());*/
        return oldJobModel;
    }

    //Return false if any container's avg delay is higher than threshold
    // and  1/(u-n)>threshold
    public boolean checkDelay(String containerId){
        double delay = modelingData.getAvgDelay(containerId, modelingData.getCurrentTime());
        double longTermDelay = modelingData.getLongTermDelay(containerId, modelingData.getCurrentTime());
        if(delay > instantaneousThreshold && longTermDelay > longTermThreshold){
            return false;
        }
        return true;
    }

    public boolean checkDelay(){
        List<String> tasks = new LinkedList<>();
        for(String containerId: containerIds){
            double delay = modelingData.getAvgDelay(containerId, modelingData.getCurrentTime());
            double arrival = modelingData.getExecutorArrivalRate(containerId, modelingData.getCurrentTime());
            double service = modelingData.getExecutorServiceRate(containerId, modelingData.getCurrentTime());
            double longtermDelay = modelingData.getLongTermDelay(containerId, modelingData.getCurrentTime());
            if(!checkDelay(containerId)){
                    writeLog("Container " + containerId
                            + " instant delay is " + delay + " exceeds threshold: " + instantaneousThreshold
                            + " longterm delay is " + longtermDelay + " exceeds threshold: " + longTermThreshold
                            + ", arrival is " + arrival + ", service is " + service);
                    return false;
            }else if(delay > instantaneousThreshold){
                tasks.add(containerId);
            }
        }
        if(tasks.size()==0)writeLog("All containers' delay is smaller than threshold");
        else writeLog("Containers delay is greater than threshold, but estimated to decrease: " + tasks);
        return true;
    }

    //Test, randomly choose one task to migrate
    public JobModel randomMoveOneTask(long time){
        updateFromJobModel(oldJobModel);
        Object [] tasks = taskContainer.keySet().toArray();
        String migrateTaskId, targetContainerId = containerIds.iterator().next();
        Random generator = new Random();
        migrateTaskId = (String)(tasks[generator.nextInt(tasks.length)]);
        String oldContainerId = taskContainer.get(migrateTaskId);
        for(String containerId: containerIds)
            if(!taskContainer.get(migrateTaskId).equals(containerId)){
                targetContainerId = containerId;
            }
        writeLog("Migrating task " + migrateTaskId + " to container " + targetContainerId);
        delayEstimator.migration(time, oldContainerId, targetContainerId, migrateTaskId);
        taskContainer.put(migrateTaskId, targetContainerId);
        return generateJobModel();
    }

    public RebalanceResult migratingOnce(){
        MigratingOnceBalancer migratingOnceBalancer = new MigratingOnceBalancer();
        migratingOnceBalancer.setModelingData(modelingData, delayEstimator, this);
        RebalanceResult result = migratingOnceBalancer.rebalance(taskContainer, instantaneousThreshold, longTermThreshold);
        return result;
    }

    public RebalanceResult scaleOutByNumber(int change){
        MigrateLargestByNumberScaler scaler = new MigrateLargestByNumberScaler();
        scaler.setModelingData(modelingData, delayEstimator, this);
        RebalanceResult result = scaler.scaleOutByNumber(taskContainer, change);
        return result;
    }

    public RebalanceResult scaleInByOne(){
        MigrateLargestByNumberScaler scaler = new MigrateLargestByNumberScaler();
        scaler.setModelingData(modelingData, delayEstimator, this);
        RebalanceResult result = scaler.scaleInByOne(taskContainer, instantaneousThreshold, longTermThreshold);
        return result;
    }
    public void updateTaskContainers(Map<String, String> newTaskContainers){
        taskContainer.clear();
        taskContainer.putAll(newTaskContainers);
    }

    public void updateOldJobModel(JobModel jobModel){
        oldJobModel = jobModel;
    }

    //Add virtual node to containerId to coordinate vn.
    /*private void addVNs(String containerId, int vnCoordinate) {
        consistentHashing.addVN(containerId, vnCoordinate);
    }*/
    /*private int getNumberOfVirtualNodes(String containerId) {
        return consistentHashing.getVNnumbers(containerId);
    }*/

    //Return 0 if in the same container, 1 otherwise
    private double migrationCost(String taskId1, String taskId2){
        if(taskContainer.get(taskId1).equals(taskContainer.get(taskId2)))return 0;
        return 1;
    }

    /*public double distance(String t1, String t2){
        double dis = consistentHashing.distance(t1,t2)+ localityWeight * (migrationCost(t1, t2)); //locality.distance(t1,t2);
        //writeLog("Overall distance between "+ t1 +" and " + t2+" is: "+dis);
        return dis;
    }*/

    public boolean retrieveArrivedAndProcessed(long time){
        boolean isMigration = false;
        //timePoints.add(time);
        if(migrationContext != null && !checkMigrationDeployed()){
            String srcId = migrationContext.getSrcContainer();
            boolean migrated = metricsListener.checkMigrated(srcId);
            if(migrated){
                //TODO:
                writeLog("Migration deployed! from container " + srcId + " Update delay estimator");
                isMigration = true;
                migrationContext.setDeployed();
                oldJobModel = newJobModel;
                updateFromJobModel(newJobModel);
                //taskContainer = newRebalanceResult.getTaskContainer();
                for(Map.Entry<String, String> entry: newRebalanceResult.getMigrationContext().getMigratingTasks().entrySet()){
                    String partition = entry.getKey();
                    String tgtContainer = entry.getValue();
                    writeLog("Migration deployed! task " + partition + " to container " + tgtContainer);
                    delayEstimator.migration(time, newRebalanceResult.getMigrationContext().getSrcContainer(), tgtContainer, partition);
                }
            }
        }
        metricsListener.updateContainerIds(containerIds);
        metricsListener.retrieveMetrics(); //Update metricsListeners' information
        //TODO: containerJobModelVersion store in where.
        return isMigration;
        /*
        //Raw information
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Arrived: " + containerArrived);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Processed: " + containerProcessed);
        */
    }

    public void updateDelay(long time){
        delayEstimator.updateAtTime(time, metricsListener.getTaskArrived(), metricsListener.getTaskProcessed(), oldJobModel);
        //For testing
        HashMap<String, Double> delays = new HashMap<>();
        for(String containerId: containerIds){
            double delay = delayEstimator.estimateDelay(containerId, time, time);
            if(delay < 0)delay = 0;
            delays.put(containerId, delay);
        }

        //Delay estimator information
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Arrived: " + delayEstimator.getExecutorsArrived(time));
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Processed: " + delayEstimator.getExecutorsCompleted(time));
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Delay: " + delays);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Partition Arrived: " + delayEstimator.getPartitionsArrived(time));
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Partition Processed: " + delayEstimator.getPartitionsCompleted(time));
    }

    public void updateModelingData(long time){
        modelingData.updateAtTime(time, metricsListener.getContainerUtilization(), oldJobModel);

        //For testing
        HashMap<String, Double> arrivalRate = new HashMap<>();
        HashMap<String, Double> serviceRate = new HashMap<>();
        HashMap<String, Double> utilization = new HashMap<>();
        HashMap<String, Double> avgDelay = new HashMap<>();
        HashMap<String, Double> longtermDelay = new HashMap<>();
        HashMap<String, Double> residual = new HashMap<>();
        HashMap<String, Double> partitionArrivalRate = new HashMap<>();
        for(String containerId: containerIds){
            double arrivalR = modelingData.getExecutorArrivalRate(containerId, time);
            arrivalRate.put(containerId, arrivalR);
            double serviceR = modelingData.getExecutorServiceRate(containerId, time);
            serviceRate.put(containerId, serviceR);
            double delay = modelingData.getAvgDelay(containerId, time);
            avgDelay.put(containerId, delay);
            delay = modelingData.getLongTermDelay(containerId, time);
            longtermDelay.put(containerId, delay);
            double res = modelingData.getAvgResidual(containerId, time);
            residual.put(containerId, res);
        }
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Arrival Rate: " + arrivalRate);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Service Rate: " + serviceRate);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Average Delay: " + avgDelay);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Longterm Delay: " + longtermDelay);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Residual: " + residual);
        for(String partitionId: tasks.keySet()){
            double arrivalR = modelingData.getPartitionArriveRate(partitionId, time);
            partitionArrivalRate.put(partitionId, arrivalR);
        }
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Partition Arrival Rate: " + partitionArrivalRate);
    }

    /*
       TODO: update all metrics and check if load is balance.
       Return false if not balance
    */
    /*
        Update taskContainer and partitionTask
     */
    private void updateFromJobModel(JobModel jobModel){
        taskContainer.clear();
        containerIds.clear();
        partitionTask.clear();
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            String container = containerModel.getProcessorId();
            containerIds.add(container);
            for(TaskModel taskModel: containerModel.getTasks().values()){
                String task = taskModel.getTaskName().getTaskName();
                taskContainer.put(task, container);
                for(SystemStreamPartition partition: taskModel.getSystemStreamPartitions()){
                    int partitionId = partition.getPartition().getPartitionId();
                    partitionTask.put(partitionId, task);
                }
            }
        }
    }
    /*public double getUtil(String processorId){
        return utilizationServer.getUtilization(processorId);
    }
    public HashMap getUtilMap(){
        return utilizationServer.getAndRemoveUtilizationMap();
    }*/
    public void showDelayMetrics(String label){
        delayEstimator.showExecutors(label);
    }

    public void updateMetrics(ConsumerRecord<String, String> record){
        snapshotMetricsRetriever.update(record);
        //TODO: update delay estimator

    }

    public JobModel getOldJobModel(){
        return oldJobModel;
    }
    private void writeLog(String log){
        System.out.println("MixedLoadBalanceManager, time " + System.currentTimeMillis() +" : " + log);
    }
}
