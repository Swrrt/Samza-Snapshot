package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.dm.MixedLoadBalanceDM.JobModelDemonstrator;
import org.apache.samza.job.dm.MixedLoadBalanceDM.KafkaOffsetRetriever;
import org.apache.samza.job.dm.MixedLoadBalanceDM.MetricsLagRetriever;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.apache.samza.zk.RMI.LocalityServer;
import org.apache.samza.config.Config;

import java.util.*;

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.zk.RMI.OffsetServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//Need to bind
public class MixedLoadBalanceManager {
    //private static final Logger LOG = LoggerFactory.getLogger(MixedLoadBalanceManager.class);
    private final int LOCALITY_RETRY_TIMES = 2;
    private double threshold;
    //TODO: reorganize all classes and tables;
    //private ConsistentHashing consistentHashing;
    //private LocalityDistance locality;  //TODO: update Locality part.
    private WebReader webReader;
    //private Map<String,List<String>> hostRack = null;
    //private Map<String,String> containerHost = null;
    private Map<String, String> taskContainer = null;
    private Set<String> containerIds = null;
    private Map<Integer, String> partitionTask = null;
    private Map<String, TaskModel> tasks; //Existing tasks
    private JobModel oldJobModel;

    // Metrics
    private Map<String, Long> taskArrived = null;
    private Map<String, Long> containerArrived = null;
    private Map<String, Long> containerArrivedTime = null;
    private Map<String, Long> taskProcessed = null;
    private Map<String, Long> containerProcessed = null;
    private Map<String, Double> taskArrivalRate = null;
    private Map<String, Double> taskBacklogs = null;
    private Map<String, Double> taskProcessingSpeed = null;
    private Map<String, Double> containerArrivalRate = null;
    private Map<String, Double> containerBacklogs = null;
    private Map<String, Double> containerProcessingSpeed = null;
    private Map<String, Long> containerFlushProcessed = null;
    private Map<String, Double > Z = null;

    private Config config;
   // private final int defaultVNs;  // Default number of VNs for new coming containers
    private final double localityWeight = 0;   // Weight parameter for Chord and Locality
    //private UtilizationServer utilizationServer = null;
    //private UnprocessedMessageMonitor unprocessedMessageMonitor = null;
    private LocalityServer localityServer = null;
    private OffsetServer offsetServer = null;
    //private final int LOCALITY_RETRY_TIMES = 1;
    //private KafkaOffsetRetriever kafkaOffsetRetriever = null;
    private MetricsLagRetriever metricsRetriever = null;
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
        metricsRetriever = new MetricsLagRetriever();
        taskArrived = new HashMap<>();
        containerArrived = new HashMap<>();
        taskProcessed = new HashMap<>();
        containerProcessed = new HashMap<>();
        taskArrivalRate = new HashMap<>();
        taskProcessingSpeed = new HashMap<>();
        taskBacklogs = new HashMap<>();
        containerArrivalRate = new HashMap<>();
        containerBacklogs = new HashMap<>();
        containerProcessingSpeed = new HashMap<>();
        containerFlushProcessed = new HashMap<>();
        containerArrivedTime = new HashMap<>();
        Z = new HashMap<>();
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
        initial(oldJobModel, config);
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
        metricsRetriever.initial(config.get("job.name"), config.get("job.loadbalance.inputtopic"));
        //kafkaOffsetRetriever.initial(config.subset("system.kafka"),config.get("job.loadbalance.inputtopic")); //TODO: need input topic name
        oldJobModel = jobModel;
        updateFromJobModel(jobModel);
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            for(Map.Entry<TaskName, TaskModel> taskModel: containerModel.getTasks().entrySet()){
                //Create new tasks model!
                tasks.put(taskModel.getKey().getTaskName(), new TaskModel(taskModel.getValue().getTaskName(),taskModel.getValue().getSystemStreamPartitions(),taskModel.getValue().getChangelogPartition()));
            }
        }
        //writeLog("Task Models:" + tasks.toString());
        setTasks(tasks);
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            insertContainer(containerModel.getProcessorId());
        }
        //unprocessedMessageMonitor.init(config.get("systems.kafka.producer.bootstrap.servers"), "metrics", config.get("job.name"));
        threshold = config.getDouble("job.loadbalance.threshold", 10.0);
        //unprocessedMessageMonitor.start();
        //utilizationServer.start();
        //localityServer.start();
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

    // Initial consisten hashing and locality when new Container comes in
    private void insertContainer(String container){
        //TODO
        writeLog("Inserting container "+container);
        //consistentHashing.insert(container, defaultVNs);
        //locality.insert(container, getContainerLocality(container), 1);
    }

    // Container left
    private void removeContainer(String container){
        //TODO
        //consistentHashing.remove(container);
        //locality.remove(container);
    }

    // Initial all tasks at the beginning;
    public void setTasks(Map<String, TaskModel> tasks){
        //consistentHashing.initTasks(tasks);
        /*for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            consistentHashing.insert(task.getKey(), 1);
            //locality.insert(task.getKey(), getTaskLocality(task.getKey()), 1);
        }*/
    }

    // Generate JobModel according to taskContainer
    public JobModel generateJobModel(){
        //generate new job model from current containers and tasks setting
        //store the new job model for future use;
        writeLog("Generating new job model...");
        writeLog("Containers: "+ containerIds);
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
            containerTasks.get(taskContainer.get(task.getKey())).add(task.getValue());
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
    public JobModel scaleOutByNumber(int change){
        writeLog("Try to scale out by: " + change);
        int currentSize = containerIds.size();
        for(int i=0;i<change; i++){
            insertContainer(String.format("%06d", currentSize + 2 + i));
            containerIds.add(String.format("%06d", currentSize + 2 + i));
        }
        return generateJobModel();
    }

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
    public void showMetrics(){
        retrieveAvgBacklog();
        retrieveArrivalRate();
        retrieveProcessingSpeed();
        retrieveArrived();
        retrieveProcessed();
        retrieveFlushProcessed();
        Map<String, Long> tt = new HashMap<>();
        for(String container: containerArrived.keySet()) {
            tt.clear();
            tt.put(container, containerArrived.get(container));
            System.out.println("MixedLoadBalanceManager, time " + containerArrivedTime.get(container) + " : " + "Arrived: " + tt);
        }
        //writeLog("Flush Processed: " + containerFlushProcessed);
        //writeLog("Backlog: " + containerBacklogs);
        //writeLog("Arrival rate: " + containerArrivalRate);
        //writeLog("Processing rate: " + containerProcessingSpeed);
    }


    public void flushMetrics(){
        writeLog("Flushing metrics");
        metricsRetriever.flush();
    }

    //Retrieve metrics (arrival rate, backlog, processing speed) and rebalance accordingly.
    public JobModel rebalanceJobModel(){
        writeLog("Try to rebalance load");
        if(checkLoad()){
            writeLog("Load is balance, no need to change job model");
            return oldJobModel;
        }
        /*
            retrieveBacklog();
            retrieveProcessingSpeed();
            Already retrieved in checkLoad()
         */

        JobModel newJobModel = generateNewJobModel(taskArrivalRate, taskBacklogs, containerProcessingSpeed, oldJobModel);
        // If scaling is needed
        if(newJobModel == null){
            writeLog("Cannot rebalance load, need to scale out");
            return null;
        }
        return newJobModel;
    }
    /*
        TODO: add a task level unprocessed message information.
     */


    /*
        Check whether the job model is still overloaded.

     */
    /*public boolean checkOverload(Map<String, Long> unprocessedMessages, Map<String, Double> processingSpeed, JobModel tryJobModel){
        Map<String, Long> unprocessedContainer = new HashMap<>();
        Map<String, String> taskContainer = getTaskContainer(tryJobModel);
        /*
         Calculate unprocessed messages for containers

        for (Map.Entry<String, Long> entry : unprocessedMessages.entrySet()) {
            String containerId = taskContainer.get(entry.getKey());
            if (!unprocessedContainer.containsKey(containerId)) {
                unprocessedContainer.put(containerId, 0l);
            }
            unprocessedContainer.put(containerId, unprocessedContainer.get(containerId) + entry.getValue());
        }
        for(String containerId: processingSpeed.keySet()){
            if(unprocessedContainer.containsKey(containerId)){
                double rate = unprocessedContainer.get(containerId)/processingSpeed.get(containerId);
                if(rate > threshold + 1e-9)return true;
            }
        }
        return false;
    }*/

    /*
        Move one task from most overloaded container to not overloaded container
        and minimize max delay.
        TODO: re-write here
        Based on littles' law and M/G/1 to estimate delay:

    */
    public Map<String, String> rebalanceByMovingOneTask(Map<String, Double> taskArrivalRate, Map<String, Double> taskBacklogs, Map<String, Double> containerProcessingSpeed, Map<String, String> oldTaskContainer) {
        int n = oldTaskContainer.size(), m = containerIds.size();
        Map<String, Double> delay = new HashMap<>();
        Map<String, Double> containerArrivalRate = new HashMap<>();
        Map<String, Double> containerBacklogs = new HashMap<>();
        for(String containerId: containerIds){
            containerArrivalRate.put(containerId, 0.0);
            containerBacklogs.put(containerId, 0.0);
        }
        //Calculate contaienrArrivalRate from taskArrivalRate and taskContainer.
        for(String taskName: taskContainer.keySet()){
            String containerId = taskContainer.get(taskName);

            double arrivalRate = taskArrivalRate.get(taskName) + containerArrivalRate.get(containerId);
            containerArrivalRate.put(containerId, arrivalRate);

            double backlog = taskBacklogs.get(taskName) + containerBacklogs.get(containerId);
            containerBacklogs.put(containerId, backlog);
        }

        //Update Z and calculate delay
        double maxDelay = -1;
        double maxExceptChoosedDelay = -1;
        String maxId = "";
        for(String containerId: containerIds){
            double backlog = containerBacklogs.get(containerId), arrival = containerArrivalRate.get(containerId), processing = containerProcessingSpeed.get(containerId);

            double de = -1.0; //-1 for no arrival
            if(arrival > 1e-9){
                de = backlog/arrival; //If there is arrival
            }

            delay.put(containerId, de);

            //Update Z in processing > arrival and arrival > 0
            if(arrival> 1e-9 && processing - arrival > 1e-9){
                Z.put(containerId, de * (processing - arrival)/ arrival);
            }

            //Choose the maximal delay and which has Z
            if(de > maxDelay && Z.containsKey(containerId)){
                if(maxDelay > maxExceptChoosedDelay){
                    maxExceptChoosedDelay = maxDelay;
                }
                maxDelay = de;
                maxId = containerId;
            }else if(de > maxExceptChoosedDelay){
                maxExceptChoosedDelay = de;
            }
        }
        // Check if the maximal delay greater than threshold
        Map<String, String> newTaskContainer = taskContainer;
        if(maxDelay > threshold){
            writeLog("Maximal delay: container" + maxId + "exceed threshold: " + maxDelay + ", now re-balance");
            String migrateTaskId = "", targetContainerId = "";
            double minimum = -100;
            for (String taskId: taskContainer.keySet())
                if((minimum < 0 || maxExceptChoosedDelay < minimum) && taskContainer.get(taskId).equals(maxId)){ //Shortcut if already have a best migration
                    double newArrival = containerArrivalRate.get(maxId) - taskArrivalRate.get(taskId);
                    double newProcess = containerProcessingSpeed.get(maxId);
                    if(newArrival < newProcess - 1e-9){
                        double newDelay = Z.get(maxId) * newArrival / (newProcess - newArrival); //Calculate new delay for source container
                        if(newDelay < minimum){
                            for(String containerId: containerIds)
                                if(!containerId.equals(maxId)){
                                    double targetArrival = containerArrivalRate.get(containerId) + taskArrivalRate.get(taskId);
                                    double targetProcess = containerProcessingSpeed.get(containerId);
                                    if(targetArrival < targetProcess - 1e-9) {
                                        double targetDelay = Z.get(containerId) * targetArrival / (targetProcess - targetArrival);
                                        if(Math.max(Math.max(targetDelay, newDelay), maxExceptChoosedDelay) < minimum){
                                            minimum = Math.max(Math.max(targetDelay, newDelay), maxExceptChoosedDelay);
                                            migrateTaskId = taskId;
                                            targetContainerId = containerId;
                                            writeLog("Find new migration way: task " + taskId + " to container " + containerId + ", new minimal delay " + minimum);
                                        }
                                    }
                                }

                        }
                    }
                }
            if(migrateTaskId.equals("")){
                writeLog("Cannot find a available migration");

                //Test
                /*Object [] tasks = taskContainer.keySet().toArray();
                targetContainerId = containerIds.iterator().next();
                Random generator = new Random();
                migrateTaskId = (String)(tasks[generator.nextInt(tasks.length)]);
                for(String containerId: containerIds)
                    if(!taskContainer.get(migrateTaskId).equals(containerId)){
                        targetContainerId = containerId;
                    }
                writeLog("Migrating task " + migrateTaskId + " to container " + targetContainerId);
                newTaskContainer.put(migrateTaskId, targetContainerId);*/

            }else{
                writeLog("Migrating task " + migrateTaskId + " to container " + targetContainerId);
                newTaskContainer.put(migrateTaskId, targetContainerId);
            }
        }else{
            writeLog("Cannot find available operator to re-balance");


            //Test, randomly choose one task to migrate
            /*Object [] tasks = taskContainer.keySet().toArray();
            String migrateTaskId, targetContainerId = containerIds.iterator().next();
            Random generator = new Random();
            migrateTaskId = (String)(tasks[generator.nextInt(tasks.length)]);
            for(String containerId: containerIds)
                if(!taskContainer.get(migrateTaskId).equals(containerId)){
                    targetContainerId = containerId;
                }
            writeLog("Migrating task " + migrateTaskId + " to container " + targetContainerId);
            newTaskContainer.put(migrateTaskId, targetContainerId);*/
        }
        return newTaskContainer;
    }
    /*
        Generate new job model with queueing theory.
        Arrival rate by tasks.
        Average backlog by tasks.
        Processing Speed by containers.
        Return null to scale up.
     */
    public JobModel generateNewJobModel(Map<String, Double> arrivalRate, Map<String, Double> backlog, Map<String, Double> processingSpeed, JobModel oldJobModel){
        /*
        Calculate VNs according to UnprocessedMessages and current processing Speed
        */
        int iterate_times = 1; // Maximal # of iteration
        for(int times = 0; times < iterate_times; times++){
            //if(!checkOverload(unprocessedMessages, processingSpeed, tryJobModel))break;
            taskContainer = rebalanceByMovingOneTask(arrivalRate, backlog, processingSpeed, taskContainer);
        }

        /*
            VNs are changed after waterfill()!
         */
        //if(checkOverload(unprocessedMessages, processingSpeed, tryJobModel)){
        //    writeLog("Cannot balance the load, need scaling out");
            /*
                TODO:
                scaling out
                Return null for scaling out
            */
        //    return null;
        //}
        return generateJobModel();
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


    public void retrieveArrived(){
        Map<Integer, Long> partitionArrived = metricsRetriever.retrieveArrived();
        Map<Integer, Long> partitionArrivedTime = metricsRetriever.retrieveArrivedTime();
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
    }

    public void retrieveFlushProcessed(){
        containerFlushProcessed.clear();
        Map<String, Long> processed =  metricsRetriever.retrieveFlushProcessed();
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

    public void retrieveProcessed(){
        taskProcessed.clear();
        containerProcessed.clear();
        Map<String, Long> processed =  metricsRetriever.retrieveProcessed();
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
    }
    /*
        Using metrics
     */
    public void retrieveAvgBacklog(){
        Map<Integer, Double> partitionBacklog = metricsRetriever.retrieveAvgBacklog();//kafkaOffsetRetriever.retrieveBacklog();
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
        Map<Integer, Double> partitionArrivalRate = metricsRetriever.retrieveArrivalRate();//kafkaOffsetRetriever.retrieveBacklog();
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
        Map<String, Double> retrieved =  metricsRetriever.retrieveProcessingSpeed();
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
    /*
        Check whether if all containers are not exceed threshold.
        Using little's law here:
        avgDelay = avgBacklog / avgArrivalRate
        Return false if avgDelay > threshold
     */
    public boolean checkLoad(){
        writeLog("Check if all containers are not overload");
        updateFromJobModel(oldJobModel);
        retrieveAvgBacklog(); //Update backlog
        retrieveArrivalRate(); //Update arrival rate
        retrieveProcessingSpeed(); //Update processing speed
        double maxDelay = -1;
        for(String containerId: containerIds){
            if(!containerBacklogs.containsKey(containerId)){
                writeLog("Cannot retrieve container "+containerId+" backlog information");
            }else if(!containerProcessingSpeed.containsKey(containerId)){
                writeLog("Cannot retrieve container "+containerId+" processing speed information");
            }else {
                double avgBacklog = containerBacklogs.get(containerId);
                double arrivalRate = containerArrivalRate.get(containerId);
                //writeLog("Container " + containerId + " average backlog: " + avgBacklog + " average arrival rate: " + arrivalRate);
                if(arrivalRate > 1e-9) {
                    if(avgBacklog / arrivalRate > maxDelay){
                        maxDelay = avgBacklog / arrivalRate;
                    }
                    if (avgBacklog / arrivalRate > threshold) {
                        writeLog("Container " + containerId + "Exceed threshold, average backlog: " + avgBacklog + ", average arrival rate: " + arrivalRate);
                        return false;
                    }
                }
            }
        }
        writeLog("Current max delay is: " + maxDelay);
        return true;
    }
    /*public double getUtil(String processorId){
        return utilizationServer.getUtilization(processorId);
    }
    public HashMap getUtilMap(){
        return utilizationServer.getAndRemoveUtilizationMap();
    }*/

    public void updateMetrics(ConsumerRecord<String, String> record){
        metricsRetriever.update(record);
    }

    public boolean readyToRebalance(){
        retrieveAvgBacklog();
        retrieveArrivalRate();
        retrieveProcessingSpeed();
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
