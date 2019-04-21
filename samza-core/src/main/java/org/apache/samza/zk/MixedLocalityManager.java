package org.apache.samza.zk;

import com.google.common.hash.Hashing;
import javafx.util.Pair;
import org.apache.samza.RMI.LocalityServer;
import org.apache.samza.RMI.UtilizationServer;
import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;

import java.util.*;

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.json.*;
import org.apache.commons.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.*;
import java.util.concurrent.ThreadLocalRandom;

public class MixedLocalityManager {
    private static final Logger LOG = LoggerFactory.getLogger(MixedLocalityManager.class);
    private double threshold;
    private class ConsistentHashing{
        private int Length;
        private Map<String, LinkedList<Integer>> coord;
        private Map<String, Integer> taskCoord;
        public ConsistentHashing(){
            coord = new HashMap<>();
        }
        // Shuffle tasks to the ring
        protected void initTasks(Map<String, TaskModel> tasks){
            Length = tasks.size();
            List<String> taskNames = new ArrayList<>(Length);
            for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
                taskNames.add(task.getKey());
            }
            Collections.shuffle(taskNames);
            taskCoord = new HashMap<>();
            for(int i=0; i < Length; i++){
                taskCoord.put(taskNames.get(i), i);
            }
        }
        // Generate hash value for strings
        private int generateHash(String item){
            //Using SHA-1 Hashing
            return Hashing.sha1().hashString(item, Charsets.UTF_8).asInt() % Length;
        }
        //Generate Virtual Node id
        private String generateVNName(String containerId, int VNid){
            return containerId + '_' + VNid;
        }
        public void insert(String item, int VNs){
            LinkedList<Integer> list = new LinkedList<>();
            for(int i=0;i<VNs;i++){
                 list.add(generateHash(generateVNName(item,i)));
            }
            coord.put(item, list);
        }
        public void remove(String item){
            coord.remove(item);
        }
        /*
            Add a VN to container
         */
        public void addVN(String containerId, int vn){
            LOG.info("Add Virtual Node at position: " + vn + " to container " + containerId);
            coord.get(containerId).add(vn);
        }
        /*
            Remove a VN from container.
         */
        public int removeVN(String containerId){
            LOG.info("Remove Virtual Node from to container " + containerId);
            int randomNum = ThreadLocalRandom.current().nextInt(0, coord.get(containerId).size());
            return coord.get(containerId).remove(randomNum);
        }
        public void simpleBalance(String maxContainer, String minContainer){
            LOG.info("Move virtual node of " + maxContainer + " to " + minContainer);
            if(coord.get(maxContainer).size()>1){
                int vn = removeVN(maxContainer);
                LOG.info("Move the VN at position: " + vn);
                addVN(minContainer, vn);
            }else LOG.info(maxContainer + " only has 1 VN. No movement");

        }
        public int distance(String itemX, String itemY){
            LOG.info("Calculate load distance between "+itemX+"  "+itemY);
            LOG.info("load items "+coord.toString());
            int min = Length + 1000;
            int x = taskCoord.get(itemX);
            LinkedList<Integer> y = coord.get(itemY);
            for(int yy: y){
                int t = x - yy;
                if(t<0)t = -t;
                if(t < min){
                    min = t;
                }
            }
            LOG.info("Calculate load distance between "+itemX+"  "+itemY + " is: " + min);
            return min;
        }
    };
    private class LocalityDistance{
        private long[] cost;
        private final int nlayer;
        private Map<String, ArrayList<String>> coord;
        private Map<String, ArrayList<String>> taskCoord; //position of task's partition
        public LocalityDistance(){
            nlayer = 5;
            cost = new long[nlayer];
            cost[0] = 1;
            for(int i= 1 ;i < nlayer ;i++)cost[i] = cost[i-1] * 10;
            coord = new HashMap<>();
        }
        // Id, Locality list (cluster, rack, server, container), Amount of state (1 for container)
        public void insert(String Id, List<String> items, Integer amount){
            LOG.info("Inserting to locality list container "+Id+" :"+items.toString());
            if(coord.containsKey(Id)){
               coord.remove(Id);
            }
            ArrayList<String> value = new ArrayList<>();
            int i = 0;
            for (String item : items) {
                value.add(item);
            }
            value.add(amount.toString());
            coord.put(Id, value);
        }
        public void remove(String Id){
            coord.remove(Id);
        }
        public long distance(String cid1, String cid2){
            long sum = 0;
            LOG.info("Calculating Locality distance between "+cid1+" "+cid2);
            LOG.info("Locality items "+coord.toString());
            ArrayList<String> v1 = coord.get(cid1), v2 = coord.get(cid2);
            LOG.info("Locality items "+v1.toString()+"      "+v2.toString());
            for(int i=0 ; i < nlayer-1; i++){
                if(!v1.get(i).equals(v2.get(i))){
                    sum += cost[i];
                }
            }
            LOG.info("Distance "+sum);
            return sum * Integer.parseInt(v1.get(nlayer-1)) * Integer.parseInt(v2.get(nlayer-1));
        }
    }
    private class WebReader{
        String hostRackUrl;
        String hostHostUrl;
        //String containerHostUrl;
        public WebReader(){
            hostRackUrl = "http://192.168.0.36:8880";
            hostHostUrl = "http://192.168.0.36:8880";
            //containerHostUrl = "http://192.168.0.36:8881";
        }
        public WebReader(String s1, String s2){
            hostRackUrl = new String(s1);
            //containerHostUrl = new String(s2);
        }
        public Map<String, List<String>> readHostRack() {
            Map<String, List<String>> hostRack = new HashMap<>();
            try{
                LOG.info("Reading Host-Rack information from ".concat(hostRackUrl));
                JSONObject json = new JSONObject(IOUtils.toString(new URL(hostRackUrl), Charset.forName("UTF-8")));
                LOG.info("Host-rack information ".concat(json.toString()));
                for(Object key: json.keySet()){
                    String keyStr = (String)key;
                    JSONArray value = json.getJSONArray(keyStr);
                    if(!hostRack.containsKey(keyStr)) {
                        hostRack.put(keyStr, new LinkedList<>());
                    }
                    for(Object o: value){
                        hostRack.get(keyStr).add((String)o);
                    }
                }
            }catch(Exception e){
                LOG.info("Error when reading Host-Rack information: " + e.toString());
            }
            return hostRack;
        }
        public Map<String, Map<String, Integer>> readHostHostDistance(){
            Map<String, Map<String, Integer>> hostHostDistance = new HashMap<>();
            try{
                LOG.info("Reading Host-Host information from ".concat(hostHostUrl));
                JSONObject json = new JSONObject(IOUtils.toString(new URL(hostHostUrl), Charset.forName("UTF-8")));
                LOG.debug("Host-Host information ".concat(json.toString()));
                for(Object key: json.keySet()){
                    String keyStr = (String)key;
                    JSONObject values = json.getJSONObject(keyStr);
                    if(!hostHostDistance.containsKey(keyStr)){
                        hostHostDistance.put(keyStr, new HashMap<>());
                    }
                    for(Object value: values.keySet()){
                        hostHostDistance.get(keyStr).put((String)value, values.getInt((String)value));
                    }
                }
            }catch(Exception e){
                LOG.info("Error when reading Host-Host information: " + e.toString());
            }
            return hostHostDistance;
        }
        public Map<String, String> readPartitionLeaderDistance(){
            Map<String, String> taskHostDistance = new HashMap<>();
            try{
                LOG.info("Reading PartitionLeader-Host information from ".concat(hostHostUrl));
                JSONObject json = new JSONObject(IOUtils.toString(new URL(hostHostUrl), Charset.forName("UTF-8")));
                LOG.debug("PartitionLeader-Host information ".concat(json.toString()));
                for(Object key: json.keySet()){
                    String keyStr = (String)key;
                    String value = json.getString(keyStr);
                    taskHostDistance.put(keyStr, value);
                }
            }catch(Exception e){
                LOG.info("Error when reading Host-Host information: " + e.toString());
            }
            return taskHostDistance;
        }
        /*public Map<String, String> readContainerHost(){
            Map<String, String> containerHost = new HashMap<>();
            try{
                LOG.info("Reading Container-Host information from ".concat(containerHostUrl));
                JSONObject json = new JSONObject(IOUtils.toString(new URL(containerHostUrl), Charset.forName("UTF-8")));
                LOG.info("Container-Host information ".concat(json.toString()));
                for(Object key: json.keySet()){
                    String keyStr = (String)key;
                    String value = json.getString(keyStr);
                    //TODO translate YARN container ID to our container ID
                    containerHost.put(keyStr.substring(keyStr.length()-6, keyStr.length()),value);
                }
            }catch(Exception e){
                LOG.info("Error: "+e.toString());
            }
            return containerHost;
        }*/

    }
    private ConsistentHashing consistentHashing;
    private LocalityDistance locality;
    private WebReader webReader;
    private Map<String,List<String>> hostRack = null;
    //private Map<String,String> containerHost = null;
    private Map<String, String> taskContainer = null;
    private Map<String, TaskModel> tasks; //Existing tasks
    private JobModel oldJobModel;
    private Config config;
    private final int defaultVNs;  // Default number of VNs for new coming containers
    private final double p1, p2;   // Weight parameter for Chord and Locality
    private UtilizationServer utilizationServer = null;
    private UnprocessedMessageMonitor unprocessedMessageMonitor = null;
    private LocalityServer localityServer = null;
    private final int LOCALITY_RETRY_TIMES = 1;
    public MixedLocalityManager(){
        config = null;
        consistentHashing = new ConsistentHashing();
        locality = new LocalityDistance();
        webReader = new WebReader();
        taskContainer = new HashMap<>();
        //containerHost = new HashMap<>();
        hostRack = new HashMap<>();
        tasks = new HashMap<>();
        oldJobModel = null;
        defaultVNs = 10;
        p1 = 1;
        p2 = 0;
        utilizationServer = new UtilizationServer();
        unprocessedMessageMonitor = new UnprocessedMessageMonitor();
        localityServer = new LocalityServer();
    }
    public void initial(JobModel jobModel, Config config){
        LOG.info("MixedLocalityManager is initializing");
        getHostRack();
        this.config = config;
        oldJobModel = jobModel;
        for(ContainerModel containerModel: jobModel.getContainers().values()){
            for(Map.Entry<TaskName, TaskModel> taskModel: containerModel.getTasks().entrySet()){
                //Create new tasks model!
                tasks.put(taskModel.getKey().getTaskName(), new TaskModel(taskModel.getValue().getTaskName(),taskModel.getValue().getSystemStreamPartitions(),taskModel.getValue().getChangelogPartition()));
                taskContainer.put(taskModel.getKey().getTaskName(), containerModel.getProcessorId());
            }
            insertContainer(containerModel.getProcessorId());
        }
        LOG.info("Task Models:" + tasks.toString());
        setTasks(tasks);
        unprocessedMessageMonitor.init(config.get("systems.kafka.producer.bootstrap.servers"), "metrics", config.get("job.name"));
        threshold = config.getDouble("balance.threshold", 10.0);
        unprocessedMessageMonitor.start();
        utilizationServer.start();
        localityServer.start();
    }
    // Read container-host mapping from web
    private Map<String, String> getContainerHost() {
        return localityServer.getLocalityMap();
    }
    // Read host-rack-cluster mapping from web
    private Map<String, List<String>> getHostRack(){
        LOG.info("Reading Host-Server-Rack-Cluster information from web");
        hostRack.putAll(webReader.readHostRack());
        LOG.info("Host-Server information:" + hostRack.toString());
        return hostRack;
    }
    /*private void updateContainerHost(){
        //TODO: add a time interval between consecutive reading
        LOG.info("Reading Container-Host information");
        if(true) {
            containerHost.putAll(getContainerHost());
        }
    }*/
    private String getContainerHost(String container){
        //TODO: If the container is not here, wait for it?
        int retry = 0;
        while(localityServer.getLocality(container) == null && retry > 0 ){
            retry--;
            try{
                Thread.sleep(500);
            }catch (Exception e){
            }
        }
        if(localityServer.getLocality(container) == null) return hostRack.keySet().iterator().next();
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
        List<String> itemLocality = (List)((LinkedList)hostRack.get(getContainerHost(container))).clone();
        itemLocality.add(0,container);
        LOG.info("Find Task " + item + " in " + itemLocality.toString());
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
        LOG.info("Inserting container "+container);
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
    private String getProcessorID(String ContainerID){
        return ContainerID.substring(ContainerID.length()-6,ContainerID.length());
        //Translate processor ID to Container ID;
        /*int retry = 10;
        while(retry >= 0){
            updateContainerHost();
            Set<String> containers = containerHost.keySet();
            LOG.info("containerID from webReader: "+containers.toString());
            for(String container: containers){
                int length = container.length();
                if(container.substring(length - 6,length).equals(processor)) return container;
            }
            LOG.info("Cannot find the containerID correspond to processor:"+ processor);
            try{
                Thread.sleep(3000);
            }catch (Exception e){
            }
        }
        return processor;*/
    }
    public JobModel generateJobModel(){
        //generate new job model from current containers and tasks setting
        //store the new job model for future use;
        LOG.info("Generating new job model...");
        LOG.info("Containers: "+ consistentHashing.coord.keySet());
        LOG.info("Tasks: "+tasks.toString());
        Map<String, LinkedList<TaskModel>> containerTasks = new HashMap<>();
        Map<String, ContainerModel> containers = new HashMap<>();
        for(String container:this.consistentHashing.coord.keySet()){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            containerTasks.put(processor, new LinkedList<>());
        }
        for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            //Find the closest container for each task
            String minContainer = null;
            double min = 0;
            for (String container: this.consistentHashing.coord.keySet()){
                LOG.info("Calculate distance between task-"+task.getKey()+" container-"+container);
                double dis = distance(task.getKey(), container);
                if(minContainer == null || dis<min){
                    minContainer = container;
                    min = dis;
                }
            }
            //containers.get(minContainer).getTasks().put(new TaskName(task.getKey()),task.getValue());
            containerTasks.get(minContainer).add(task.getValue());
        }
        for(String container:this.consistentHashing.coord.keySet()){
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
        LOG.info("New job model:" + oldJobModel.toString());
        return oldJobModel;
    }
    // Generate new Job Model based on new processors list
    public JobModel generateNewJobModel(List<String> processors){
        Set<String> containers = new HashSet<>();
        //TODO: Translate from processorID to container ID
        LOG.info("Generating new job model from processors:" + processors.toString());
        for(String processor: processors){
            containers.add(processor);
            //Insert new container
            if(!this.consistentHashing.coord.containsKey(processor)){
                insertContainer(processor);

            }
        }
        //Remove containers no longer exist
        for(String container: this.consistentHashing.coord.keySet()){
            if(!containers.contains(container)){
                removeContainer(container);
            }
        }
        return generateJobModel();
    }
    public JobModel rebalanceJobModel(){
        LOG.info("Rebalancing");
        /*
        HashMap util = utilizationServer.getAndRemoveUtilizationMap();
        LOG.info("Utilization map is: " + util.toString());
        */
        //Using UnprocessedMessages to rebalance
        HashMap unprocessedMessages = unprocessedMessageMonitor.getUnprocessedMessage();
        HashMap processingSpeed = unprocessedMessageMonitor.getProcessingSpeed();
        LOG.info("Unprocessed Messages information: " + unprocessedMessages.toString());
        LOG.info("Processing speed information: " + processingSpeed.toString());
        return generateNewJobModel(unprocessedMessages, processingSpeed, oldJobModel);
    }
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
            LOG.info("Total Processing Speed is too low, no change is made");
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
        LOG.info("Current max backlog/process container: " + maxContainer);
        /*
            Move virtual nodes from most overloaded container
         */
        double perVN = max / getNumberOfVirtualNodes(maxContainer.substring(16));
        double unprocVN = unprocessedContainer.get(maxContainer) / getNumberOfVirtualNodes(maxContainer.substring(16));
        if (max < threshold - 1e-9) {
            LOG.info("No need to move");
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
        //TODO
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
            LOG.info("Cannot balance the load, need scaling out");
            /*
                TODO:
                scaling out
             */
        }
        return tryJobModel;
    }
    private void addVNs(String containerId, int vnCoordinate) {
        consistentHashing.addVN(containerId, vnCoordinate);
    }
    private int getNumberOfVirtualNodes(String containerId) {
        return consistentHashing.coord.get(containerId).size();
    }
    public double distance(String t1, String t2){
        double dis = p1*consistentHashing.distance(t1,t2)+p2*locality.distance(t1,t2);
        LOG.info("Overall distance between "+ t1 +" and " + t2+" is: "+dis);
        return dis;
    }
    /*public double getUtil(String processorId){
        return utilizationServer.getUtilization(processorId);
    }
    public HashMap getUtilMap(){
        return utilizationServer.getAndRemoveUtilizationMap();
    }*/
}
