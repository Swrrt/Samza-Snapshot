package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.dm.MixedLoadBalanceDM.*;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.apache.samza.zk.RMI.LocalityServer;
import org.apache.samza.config.Config;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.zk.RMI.MetricsClient;
import org.apache.samza.zk.RMI.OffsetServer;

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
    private Map<String, Long> taskArrived = null;
    private Map<String, Long> containerArrived = null;
    private Map<String, Long> taskProcessed = null;
    private Map<String, Long> containerProcessed = null;
    private Map<String, Double> containerUtilization = null;
    private Map<String, Double> taskArrivalRate = null;
    private Map<String, Double> taskBacklogs = null;
    private Map<String, Double> taskProcessingSpeed = null;
    private Map<String, Long> containerFlushProcessed = null;
    //private Map<String, ContainerMigratingState> containerMigratingState = null;
    private Map<String, Long> containerJobModelVersion = null;
    private MigrationContext migrationContext = null;
    private ModelingData modelingData;
    private Config config;
   // private final int defaultVNs;  // Default number of VNs for new coming containers
    private final double localityWeight = 0;   // Weight parameter for Chord and Locality
    //private UtilizationServer utilizationServer = null;
    //private UnprocessedMessageMonitor unprocessedMessageMonitor = null;
    private LocalityServer localityServer = null;
    private OffsetServer offsetServer = null;
    //private final int LOCALITY_RETRY_TIMES = 1;
    //private KafkaOffsetRetriever kafkaOffsetRetriever = null;
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
        localityServer = new LocalityServer();
        offsetServer = new OffsetServer();
        //kafkaOffsetRetriever = new KafkaOffsetRetriever();
        snapshotMetricsRetriever = new SnapshotMetricsRetriever();
        delayEstimator = new DelayEstimator();
        taskArrived = new HashMap<>();
        containerArrived = new HashMap<>();
        taskProcessed = new HashMap<>();
        containerProcessed = new HashMap<>();
        containerUtilization = new HashMap<>();
        taskArrivalRate = new HashMap<>();
        taskProcessingSpeed = new HashMap<>();
        taskBacklogs = new HashMap<>();
        containerFlushProcessed = new HashMap<>();
        //containerMigratingState = new ConcurrentHashMap<>();
        containerJobModelVersion = new HashMap<>();
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
        for(String containerId: containerIds){
            containerJobModelVersion.put(containerId, -1l);
        }
        //unprocessedMessageMonitor.start();
        //utilizationServer.start();
        localityServer.start();
        offsetServer.start();
    }
    // Read container-host mapping from web
    /*private Map<String, String> getContainerHost() {
        return localityServer.getLocalityMap();
    }*/
    // Read host-rack-cluster mapping from web
    /*private Map<String, List<String>> getHostRack(){
        writeLog("Reading Host-Server-Rack-Cluster information from web");
        hostRack.putAll(webReader.readHostRack());
        writeLog("Host-Server information:" + hostRack.toString());
        return hostRack;
    }
    private String getContainerHost(String container){
        return getContainerHost(container, LOCALITY_RETRY_TIMES);
    }*/
    /*private String getContainerHost(String container, int retryTimes){
        //TODO: If the container is not here, wait for it?
        int retry = retryTimes; //Number of times to retry
        while(localityServer.getLocality(container) == null && retry > 0 ){
            retry--;
            try{
                Thread.sleep(500);
            }catch (Exception e){
            }
        }
        if(localityServer.getLocality(container) == null) {
            writeLog("Cannot get locality information of container " + container);
            return hostRack.keySet().iterator().next();
        }
        return localityServer.getLocality(container);
        /*if(containerHost.containsKey(container))return containerHost.get(container);
        else {
            return containerHost.values().iterator().next();
        }
    }*/

    // Construct the container-(container, host, rack cluster) mapping
    /*private List<String> getContainerLocality(String item){
        //updateContainerHost();
        getHostRack();
        List<String> itemLocality = (List)((LinkedList)hostRack.get(getContainerHost(item))).clone();
        itemLocality.add(0,item);
        return itemLocality;
    }*/

    // Construct the task-(container, host, rack cluster) mapping
    /*private List<String> getTaskLocality(String item){
        //updateContainerHost();
        getHostRack();
        String container = taskContainer.get(item);
        List<String> itemLocality = (List)((LinkedList)hostRack.get(getContainerHost(container, 0))).clone();
        itemLocality.add(0,container);
        writeLog("Find Task " + item + " in " + itemLocality.toString());
        return itemLocality;
    }*/

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

    // Scale out by change number containers, assign default number of VNs to them and generate new Job Model


    //Choose first change# containers from containerIds and remove them.
    /*public JobModel scaleInByNumber(int change){
        writeLog("Try to scale in by: " + change);
        int currentSize = containerIds.size();
        if(currentSize <= change){
            writeLog("Only have " + currentSize + " containers, should leave one container");
            change = currentSize - 1;
        }
        for(int i=0;i<change;i++){
            String containerId = containerIds.iterator().next();
            removeContainer(containerId);
            containerIds.remove(containerId);
        }
        return generateJobModel();
    }*/

    // Generate new Job Model based on new processors list
    /*public JobModel generateNewJobModel(List<String> processors){
        Set<String> containers = new HashSet<>();
        //TODO: Translate from processorID to container ID
        writeLog("Generating new job model from processors:" + processors.toString());
        for(String processor: processors){
            containers.add(processor);
            //Insert new container
            if(!containerIds.contains(processor)){
                insertContainer(processor);

            }
        }
        //Remove containers no longer exist
        for(String container: containerIds){
            if(!containers.contains(container)){
                removeContainer(container);
            }
        }
        return generateJobModel();
    }*/
    /*public void showMetrics(){
        //retrieveArrivedAndProcessed();
        //retrieveAvgBacklog();
        //retrieveArrivalRate();
        //retrieveProcessingSpeed();
        //retrieveArrived();
        //retrieveProcessed();
        retrieveFlushProcessed();
        Map<String, Long> tt = new HashMap<>();
        for(String container: containerArrived.keySet()) {
            tt.clear();
            tt.put(container, containerArrived.get(container));
            System.out.println("MixedLoadBalanceManager, time " + containerArrivedTime.get(container) + " : " + "Arrived: " + tt);
            tt.clear();
            tt.put(container, containerProcessed.get(container));
            System.out.println("MixedLoadBalanceManager, time " + containerArrivedTime.get(container) + " : " + "Processed: " + tt);
        }
        //writeLog("Flush Processed: " + containerFlushProcessed);
        //writeLog("Backlog: " + containerBacklogs);
        //writeLog("Arrival rate: " + containerArrivalRate);
        //writeLog("Processing rate: " + containerProcessingSpeed);
    }*/


    /*public void flushMetrics(){
        writeLog("Flushing metrics");
        snapshotMetricsRetriever.flush();
    }*/

    //Retrieve metrics (arrival rate, backlog, processing speed) and rebalance accordingly.

    /*
        Generate new job model with queueing theory.
        Arrival rate by tasks.
        Average backlog by tasks.
        Processing Speed by containers.
        Return null to scale up.
     */

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
        HashMap<String, String> offsets;
        boolean isMigration = false;
        //timePoints.add(time);
        taskProcessed.clear();
        taskArrived.clear();
        containerProcessed.clear();
        containerArrived.clear();
        containerUtilization.clear();

        if(migrationContext != null && !checkMigrationDeployed()){
            String srcId = migrationContext.getSrcContainer();
            MetricsClient client = new MetricsClient(localityServer.getLocality(srcId), 8900 + Integer.parseInt(srcId), srcId);
            offsets = client.getOffsets();
            long jobModelVersion = -1;
            if(offsets != null && offsets.containsKey("JobModelVersion")){
                jobModelVersion = Long.parseLong(offsets.get("JobModelVersion"));
            }
            //Update container JobModelVersion
            long oldJobModelVersion = containerJobModelVersion.getOrDefault(srcId, -1l);
            if(jobModelVersion > -1){
                if(jobModelVersion > oldJobModelVersion){
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
        }

        MetricsMetadata metricsMetadata = MixedLoadBalanceMetricsListener.retrieveArrivedAndProcessed(time, containerIds, localityServer, offsetServer, containerJobModelVersion);
        taskProcessed = metricsMetadata.getTaskProcessed();
        taskArrived = metricsMetadata.getTaskArrived();
        containerProcessed = metricsMetadata.getTaskProcessed();
        containerArrived = metricsMetadata.getContainerArrived();
        containerUtilization = metricsMetadata.getContainerUtilization();
        //TODO: containerJobModelVersion store in where.

        /*for(String containerId: containerIds){
            MetricsClient client = new MetricsClient(localityServer.getLocality(containerId), 8900 + Integer.parseInt(containerId), containerId);
            offsets = client.getOffsets();
            double utilization = -100;
            long jobModelVersion = -1;
            //Update container JobModelVersion
            if(offsets != null && offsets.containsKey("JobModelVersion")){
                jobModelVersion = Long.parseLong(offsets.get("JobModelVersion"));
                offsets.remove("JobModelVersion");
            }
            long oldJobModelVersion = containerJobModelVersion.getOrDefault(containerId, -1l);
            if(jobModelVersion > -1 && jobModelVersion > oldJobModelVersion){
                containerJobModelVersion.put(containerId, jobModelVersion);
            }
            if(offsets != null && offsets.containsKey("Utilization")) {
                utilization = Double.parseDouble(offsets.get("Utilization"));
                offsets.remove("Utilization");
            }
            if(utilization > -1e-9){ //Online
                containerUtilization.put(containerId, utilization);
            }else { //Offline
                containerUtilization.put(containerId, 0.0);
            }

            long s_arrived = 0, s_processed = 0;
            for(Map.Entry<String, String> entry: offsets.entrySet()){
                String id = entry.getKey();
                String value = entry.getValue();
                int i = value.indexOf('_');
                long begin = offsetServer.getBeginOffset(id);
                long arrived = Long.parseLong(value.substring(0, i)) - begin - 1, processed = Long.parseLong(value.substring(i+1)) - begin;
                if(arrived < 0) arrived = 0;
                if(processed < 0) processed = 0;
                //delayEstimator.updatePartitionArrived(id, time, arrived);
                long t = taskArrived.getOrDefault(id, 0l);
                if(t > arrived) arrived = t;
                taskArrived.put(id, arrived);
                //delayEstimator.updatePartitionCompleted(id, time, processed);
                t = taskProcessed.getOrDefault(id, 0l);
                if(t > processed) processed = t;
                taskProcessed.put(id, processed);
                //delayEstimator.updatePartitionBacklog(id, time, containerId, arrived - processed);
                s_arrived += arrived;
                s_processed += processed;
            }
            containerArrived.put(containerId, s_arrived);
            containerProcessed.put(containerId, s_processed);
        }*/
        return isMigration;

        /*
        //Raw information
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Arrived: " + containerArrived);
        System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Processed: " + containerProcessed);
        */
    }

    public void updateDelay(long time){
        delayEstimator.updateAtTime(time, taskArrived, taskProcessed, oldJobModel);

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
        modelingData.updateAtTime(time, containerUtilization, oldJobModel);

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
    /*public void retrieveArrived(){
        Map<Integer, Long> partitionArrived = snapshotMetricsRetriever.retrieveArrived();
        Map<Integer, Long> partitionArrivedTime = snapshotMetricsRetriever.retrieveArrivedTime();
        taskArrived.clear();
        containerArrived.clear();
        containerArrivedTime.clear();
        for(Map.Entry<Integer, Long> entry: partitionArrived.entrySet()){
            int partition = entry.getKey();
            long arrived = entry.getValue();
            String task = partitionTask.get(partition);
            taskArrived.put(task, arrived);
            String container = taskContainer.get(task);
            if(containerArrived.containsKey(container)){
                arrived += containerArrived.get(container);
            }
            containerArrived.put(container, arrived);
        }
        for(Map.Entry<Integer, Long> entry: partitionArrivedTime.entrySet()){
            int partition = entry.getKey();
            long time = entry.getValue();
            String task = partitionTask.get(partition);
            String container = taskContainer.get(task);
            if(containerArrivedTime.containsKey(container)){
                long beforeTime = containerArrivedTime.get(container);
                if(beforeTime > time)time = beforeTime;
            }
            containerArrivedTime.put(container, time);
        }
    }*/

    public void retrieveFlushProcessed(){
        containerFlushProcessed.clear();
        Map<String, Long> processed =  snapshotMetricsRetriever.retrieveFlushProcessed();
        for(Map.Entry<String, Long> entry: processed.entrySet()){
            String container = entry.getKey();
            long processe = entry.getValue();
            /*String container = taskContainer.get(task);
            if(containerFlushProcessed.containsKey(container)){
                processe += containerFlushProcessed.get(container);
            }*/
            containerFlushProcessed.put(container, processe);
        }
    }

    /*public void retrieveProcessed(){
        taskProcessed.clear();
        containerProcessed.clear();
        Map<String, Long> processed =  snapshotMetricsRetriever.retrieveProcessed();
        for(Map.Entry<String, Long> entry: processed.entrySet()){
            String task = entry.getKey();
            long processe = entry.getValue();
            taskProcessed.put(task, processe);
            String container = taskContainer.get(task);
            if(containerProcessed.containsKey(container)){
                processe += containerProcessed.get(container);
            }
            containerProcessed.put(container, processe);
        }
    }*/
    /*
        Using metrics
     */
    /*public void retrieveAvgBacklog(){
        Map<Integer, Double> partitionBacklog = snapshotMetricsRetriever.retrieveAvgBacklog();//kafkaOffsetRetriever.retrieveBacklog();
        taskBacklogs.clear();
        containerBacklogs.clear();
        for(Map.Entry<Integer, Double> entry: partitionBacklog.entrySet()){
            int partition = entry.getKey();
            double backlog = entry.getValue();
            String task = partitionTask.get(partition);
            taskBacklogs.put(task, backlog);
            String container = taskContainer.get(task);
            if(containerBacklogs.containsKey(container)){
                backlog += containerBacklogs.get(container);
            }
            containerBacklogs.put(container, backlog);
        }
        writeLog("Average Backlog: " + containerBacklogs);
    }
    public void retrieveArrivalRate(){
        Map<Integer, Double> partitionArrivalRate = snapshotMetricsRetriever.retrieveArrivalRate();//kafkaOffsetRetriever.retrieveBacklog();
        taskArrivalRate.clear();
        containerArrivalRate.clear();
        for(Map.Entry<Integer, Double> entry: partitionArrivalRate.entrySet()){
            int partition = entry.getKey();
            double backlog = entry.getValue();
            String task = partitionTask.get(partition);
            taskArrivalRate.put(task, backlog);
            String container = taskContainer.get(task);
            if(containerArrivalRate.containsKey(container)){
                backlog += containerArrivalRate.get(container);
            }
            containerArrivalRate.put(container, backlog);
        }
        writeLog("Arrival Rate: " + containerArrivalRate);
    }
    public void retrieveProcessingSpeed(){
        taskProcessingSpeed.clear();
        containerProcessingSpeed.clear();
        Map<String, Double> retrieved =  snapshotMetricsRetriever.retrieveProcessingSpeed();
        for(Map.Entry<String, Double> entry: retrieved.entrySet()){
            String task = entry.getKey();
            double speed = entry.getValue();
            taskProcessingSpeed.put(task, speed);
            String container = taskContainer.get(task);
            if(containerProcessingSpeed.containsKey(container)){
                speed += containerProcessingSpeed.get(container);
            }
            containerProcessingSpeed.put(container, speed);
        }
        writeLog("Processing Speed: " + containerProcessingSpeed);
    }*/

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

    public boolean readyToRebalance(){
       /* retrieveAvgBacklog();
        retrieveArrivalRate();
        retrieveProcessingSpeed();*/
        if(taskBacklogs.size() == taskContainer.size() && taskProcessingSpeed.size() == taskContainer.size() && taskArrivalRate.size() == taskContainer.size()){
            writeLog("Ready to rebalance");
            return true;
        }
        writeLog("Not ready to rebalance, # of tasks: " + taskContainer.size() + ", backlogs size: "+taskBacklogs.size()+" processing speed size: "+taskProcessingSpeed.size() + " arrival rate size: " + taskArrivalRate.size());

        return false;
    }
    public JobModel getOldJobModel(){
        return oldJobModel;
    }
    private void writeLog(String log){
        System.out.println("MixedLoadBalanceManager, time " + System.currentTimeMillis() +" : " + log);
    }
}
