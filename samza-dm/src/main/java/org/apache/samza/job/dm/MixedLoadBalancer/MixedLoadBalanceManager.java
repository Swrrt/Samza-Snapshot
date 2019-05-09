package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.coordinator.JobModelManager;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//Need to bind
public class MixedLoadBalanceManager {
    //private static final Logger LOG = LoggerFactory.getLogger(MixedLoadBalanceManager.class);
    private final int LOCALITY_RETRY_TIMES = 2;
    private double threshold;
    //TODO: reorganize all classes and tables;
    private ConsistentHashing consistentHashing;
    private LocalityDistance locality;  //TODO: update Locality part.
    private WebReader webReader;
    private Map<String,List<String>> hostRack = null;
    //private Map<String,String> containerHost = null;
    private Map<String, String> taskContainer = null;
    private Map<Integer, String> partitionTask = null;
    private Map<String, TaskModel> tasks; //Existing tasks
    private JobModel oldJobModel;
    private Map<String, Long> taskBacklogs = null;
    private Map<String, Double> taskProcessingSpeed = null;
    private Map<String, Long> containerBacklogs = null;
    private Map<String, Double> containerProcessingSpeed = null;
    private Config config;
    private final int defaultVNs;  // Default number of VNs for new coming containers
    private final double localityWeight;   // Weight parameter for Chord and Locality
    //private UtilizationServer utilizationServer = null;
    //private UnprocessedMessageMonitor unprocessedMessageMonitor = null;
    private LocalityServer localityServer = null;
    //private final int LOCALITY_RETRY_TIMES = 1;
    //private KafkaOffsetRetriever kafkaOffsetRetriever = null;
    private MetricsLagRetriever metricsRetriever = null;
    public MixedLoadBalanceManager(){
        config = null;
        consistentHashing = new ConsistentHashing();
        locality = new LocalityDistance();
        webReader = new WebReader();
        partitionTask = new HashMap<>();
        taskContainer = new HashMap<>();
        //containerHost = new HashMap<>();
        hostRack = new HashMap<>();
        tasks = new HashMap<>();
        oldJobModel = null;
        defaultVNs = 10;
        localityWeight = 0;
        //utilizationServer = new UtilizationServer();
        //unprocessedMessageMonitor = new UnprocessedMessageMonitor();
        localityServer = new LocalityServer();
        //kafkaOffsetRetriever = new KafkaOffsetRetriever();
        metricsRetriever = new MetricsLagRetriever();
        taskProcessingSpeed = new HashMap<>();
        taskBacklogs = new HashMap<>();
        containerBacklogs = new HashMap<>();
        containerProcessingSpeed = new HashMap<>();
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
        getHostRack();
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
        writeLog("Task Models:" + tasks.toString());
        setTasks(tasks);
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            insertContainer(containerModel.getProcessorId());
        }
        //unprocessedMessageMonitor.init(config.get("systems.kafka.producer.bootstrap.servers"), "metrics", config.get("job.name"));
        threshold = config.getDouble("job.loadbalance.threshold", 10.0);
        //unprocessedMessageMonitor.start();
        //utilizationServer.start();
        localityServer.start();
    }
    // Read container-host mapping from web
    /*private Map<String, String> getContainerHost() {
        return localityServer.getLocalityMap();
    }*/
    // Read host-rack-cluster mapping from web
    private Map<String, List<String>> getHostRack(){
        writeLog("Reading Host-Server-Rack-Cluster information from web");
        hostRack.putAll(webReader.readHostRack());
        writeLog("Host-Server information:" + hostRack.toString());
        return hostRack;
    }
    private String getContainerHost(String container){
        return getContainerHost(container, LOCALITY_RETRY_TIMES);
    }
    private String getContainerHost(String container, int retryTimes){
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
        }*/
    }

    // Construct the container-(container, host, rack cluster) mapping
    private List<String> getContainerLocality(String item){
        //updateContainerHost();
        getHostRack();
        List<String> itemLocality = (List)((LinkedList)hostRack.get(getContainerHost(item))).clone();
        itemLocality.add(0,item);
        return itemLocality;
    }

    // Construct the task-(container, host, rack cluster) mapping
    private List<String> getTaskLocality(String item){
        //updateContainerHost();
        getHostRack();
        String container = taskContainer.get(item);
        List<String> itemLocality = (List)((LinkedList)hostRack.get(getContainerHost(container, 0))).clone();
        itemLocality.add(0,container);
        writeLog("Find Task " + item + " in " + itemLocality.toString());
        return itemLocality;
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

    // New Container comes in;
    private void insertContainer(String container){
        //TODO
        writeLog("Inserting container "+container);
        consistentHashing.insert(container, defaultVNs);
        locality.insert(container, getContainerLocality(container), 1);
    }

    // Container left
    private void removeContainer(String container){
        //TODO
        consistentHashing.remove(container);
        locality.remove(container);
    }

    // Initial all tasks at the beginning;
    public void setTasks(Map<String, TaskModel> tasks){
        consistentHashing.initTasks(tasks);
        for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            consistentHashing.insert(task.getKey(), 1);
            locality.insert(task.getKey(), getTaskLocality(task.getKey()), 1);
        }
    }

    // Generate job model based on current vn and locality information
    public JobModel generateJobModel(){
        //generate new job model from current containers and tasks setting
        //store the new job model for future use;
        writeLog("Generating new job model...");
        writeLog("Containers: "+ taskContainer.values());
        writeLog("Tasks: "+ taskContainer.keySet());
        Map<String, LinkedList<TaskModel>> containerTasks = new HashMap<>();
        Map<String, ContainerModel> containers = new HashMap<>();
        for(String container: taskContainer.values()){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            containerTasks.put(processor, new LinkedList<>());
        }
        for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            //Find the closest container for each task
            String minContainer = null;
            double min = 0;
            for (String container: taskContainer.values()){
                writeLog("Calculate distance between task-"+task.getKey()+" container-"+container);
                double dis = distance(task.getKey(), container);
                if(minContainer == null || dis<min){
                    minContainer = container;
                    min = dis;
                }
            }
            //containers.get(minContainer).getTasks().put(new TaskName(task.getKey()),task.getValue());
            containerTasks.get(minContainer).add(task.getValue());
        }
        for(String container: taskContainer.values()){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            Map<TaskName, TaskModel> tasks = new HashMap<>();
            for(TaskModel task: containerTasks.get(container)){
                tasks.put(task.getTaskName(),task);
            }
            containers.put(processor, new ContainerModel(processor,0, tasks));
        }
        oldJobModel = new JobModel(config, containers);
        taskContainer = getTaskContainer(oldJobModel);
        writeLog("New job model:" + oldJobModel.toString());
        return oldJobModel;
    }
    public JobModel scaleUpByNumber(int change){
        int currentSize = oldJobModel.getContainers().size();
        for(int i=0;i<change; i++){
            insertContainer(String.format("%06d", currentSize + 2 + i));
        }
        return generateJobModel();
    }
    // Generate new Job Model based on new processors list
    public JobModel generateNewJobModel(List<String> processors){
        Set<String> containers = new HashSet<>();
        //TODO: Translate from processorID to container ID
        writeLog("Generating new job model from processors:" + processors.toString());
        for(String processor: processors){
            containers.add(processor);
            //Insert new container
            if(!taskContainer.values().contains(processor)){
                insertContainer(processor);

            }
        }
        //Remove containers no longer exist
        for(String container: taskContainer.values()){
            if(!containers.contains(container)){
                removeContainer(container);
            }
        }
        return generateJobModel();
    }
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

        JobModel newJobModel = generateNewJobModel(taskBacklogs, containerProcessingSpeed, oldJobModel);
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
    public boolean checkOverload(Map<String, Long> unprocessedMessages, Map<String, Double> processingSpeed, JobModel tryJobModel){
        Map<String, Long> unprocessedContainer = new HashMap<>();
        Map<String, String> taskContainer = getTaskContainer(oldJobModel);
        /*
         Calculate unprocessed messages for containers
         */
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
    }
    /*
        Water filling to move VN from most overloaded container to not overloaded container
        Minimize max(unproc/proc)
     */

    public JobModel waterfill(Map<String, Long> unprocessedMessages, Map<String, Double> processingSpeed, JobModel oldJobModel) {
        long totalUnproc = 0;
        int number = unprocessedMessages.size();
        double totalProc = 0;
        Map<String, Long> unprocessedContainer = new HashMap<>();
        Map<String, String> taskContainer = getTaskContainer(oldJobModel);

        /*
         Calculate unprocessed messages for containers
         */
        for (Map.Entry<String, Long> entry : unprocessedMessages.entrySet()) {
            totalUnproc += entry.getValue();
            String containerId = taskContainer.get(entry.getKey());
            if (!unprocessedContainer.containsKey(containerId)) {
                unprocessedContainer.put(containerId, 0l);
            }
            unprocessedContainer.put(containerId, unprocessedContainer.get(containerId) + entry.getValue());
        }

        for (Double x : processingSpeed.values()) {
            totalProc += x;
        }

        if (totalProc < 1e-9) {
            writeLog("Total Processing Speed is too low, no change is made");
            return generateJobModel();
        }
        /*
            Choose most overloaded container
         */
        double max = -1;
        String maxContainer = "";
        for (Map.Entry<String, Double> entry : processingSpeed.entrySet()) {
            String keyName = entry.getKey();
            Double speed = entry.getValue();
            if (speed > 1e-9 && unprocessedContainer.containsKey(keyName)) {
                long unprocessed = unprocessedContainer.get(keyName);
                if (unprocessed / speed > max) {
                    max = unprocessed / speed;
                    maxContainer = keyName;
                }
            }
        }
        writeLog("Current max backlog/process container: " + maxContainer);
        /*
            Move virtual nodes from most overloaded container
         */
        double perVN = max / getNumberOfVirtualNodes(maxContainer);
        double unprocVN = unprocessedContainer.get(maxContainer) / getNumberOfVirtualNodes(maxContainer);
        if (max < threshold - 1e-9) {
            writeLog("No need to move");
            return oldJobModel;
        }
        long moveVNs = (long) Math.ceil((max - threshold) / perVN);
        /*
            Choose the container which estimate minimize the backlog/process to put
            TODO: use real backlog/process

        */
        for (int i = 0; i < moveVNs; i++) {
            int vn = consistentHashing.removeVN(maxContainer);
            String minContainer = maxContainer;
            double min = threshold + 100;
            for (String container : unprocessedContainer.keySet())
                if (!container.equals(maxContainer) && processingSpeed.containsKey(container)) {
                    double proc = processingSpeed.get(container);
                    double unproc = unprocessedContainer.get(container) + unprocVN;
                    if (proc > 1e-9 && unproc / proc < min) {
                        min = unproc / proc;
                        minContainer = container;
                    }

                }
            addVNs(minContainer, vn);
        }
        return generateJobModel();
    }
    /*
        Generate new job model with UnprocessedMessages information.
        Unprocessed Messages by tasks.
        Processing Speed by containers.
        Return true to scale up.
     */
    public JobModel generateNewJobModel(Map<String, Long> unprocessedMessages, Map<String, Double> processingSpeed, JobModel oldJobModel){
        /*
        Calculate VNs according to UnprocessedMessages and current processing Speed
        */
        int iterate_times = 1; // Maximal # of iteration
        JobModel tryJobModel = oldJobModel;
        for(int times = 0; times < iterate_times; times++){
            if(!checkOverload(unprocessedMessages, processingSpeed, tryJobModel))break;
            tryJobModel = waterfill(unprocessedMessages, processingSpeed, tryJobModel);
        }
        /*
            VNs are changed after waterfill()!
         */
        if(checkOverload(unprocessedMessages, processingSpeed, tryJobModel)){
            writeLog("Cannot balance the load, need scaling out");
            /*
                TODO:
                scaling out
                Return null for scaling out
            */
            return null;
        }
        return tryJobModel;
    }
    //Add virtual node to containerId to coordinate vn.
    private void addVNs(String containerId, int vnCoordinate) {
        consistentHashing.addVN(containerId, vnCoordinate);
    }
    private int getNumberOfVirtualNodes(String containerId) {
        return consistentHashing.getVNnumbers(containerId);
    }
    public double distance(String t1, String t2){
        double dis = consistentHashing.distance(t1,t2)+localityWeight*locality.distance(t1,t2);
        writeLog("Overall distance between "+ t1 +" and " + t2+" is: "+dis);
        return dis;
    }

    /*
        Using kafka-consumer-groups command to run
     */
    public void retrieveBacklog(){
        Map<Integer, Long> partitionBacklog = metricsRetriever.retrieveBacklog();//kafkaOffsetRetriever.retrieveBacklog();
        taskBacklogs.clear();
        containerBacklogs.clear();
        for(Map.Entry<Integer, Long> entry: partitionBacklog.entrySet()){
            int partition = entry.getKey();
            long backlog = entry.getValue();
            String task = partitionTask.get(partition);
            taskBacklogs.put(task, backlog);
            String container = taskContainer.get(task);
            if(containerBacklogs.containsKey(container)){
                backlog += containerBacklogs.get(container);
            }
            containerBacklogs.put(container, backlog);
        }
    }
    public void retrieveProcessingSpeed(){
        taskProcessingSpeed.clear();
        containerProcessingSpeed.clear();
        Map<String, Double> retrieved =  metricsRetriever.retrieveSpeed();
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
        partitionTask.clear();
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            String container = containerModel.getProcessorId();
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
     */
    public boolean checkLoad(){
        writeLog("Check if all containers are not overload");
        updateFromJobModel(oldJobModel);
        retrieveBacklog(); //Update backlog
        retrieveProcessingSpeed(); //Update processing speed
        for(String containerId: taskContainer.values()){
            if(!containerBacklogs.containsKey(containerId)){
                writeLog("Cannot retrieve container "+containerId+" backlog information");
            }else if(!containerProcessingSpeed.containsKey(containerId)){
                writeLog("Cannot retrieve container "+containerId+" processing speed information");
            }else {
                long backlog = containerBacklogs.get(containerId);
                double processSpeed = containerProcessingSpeed.get(containerId);
                writeLog("Container " + containerId + " backlog: " + backlog + " processing speed: " + processSpeed);
                if (backlog / processSpeed > threshold) {
                    writeLog("Container " + containerId + "Exceed threshold, backlog: " + backlog + ", processing speed: " + processSpeed);
                    return false;
                }
            }
        }
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
        retrieveBacklog();
        retrieveProcessingSpeed();
        if(taskBacklogs.size() == taskContainer.size() && taskProcessingSpeed.size() == taskContainer.size()){
            writeLog("Ready to rebalance");
            return true;
        }
        writeLog("Not ready to rebalance, backlogs size: "+taskBacklogs.size()+" processing speed size: "+taskProcessingSpeed.size());
        return false;
    }
    public JobModel getOldJobModel(){
        return oldJobModel;
    }
    private void writeLog(String log){
        System.out.println("MixedLoadBalanceManager: " + log);
    }
}
