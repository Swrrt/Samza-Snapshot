package org.apache.samza.zk;

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

public class MixedLocalityManager {
    private static final Logger LOG = LoggerFactory.getLogger(MixedLocalityManager.class);
    private static final int LOWERBOUND = 10;
    private static final int UPPERBOUND = 200;
    private class ChordHashing{
        private final int Length;
        private Map<String, LinkedList<Integer>> coord;
        public ChordHashing(){
            Length = 100000007;
            coord = new HashMap<>();
        }
        public ChordHashing(int L){
            Length = L;
            coord = new HashMap<>();
        }
        // Generate hash value for strings
        private int generateHash(String item){
            Random t = new Random();
            return t.nextInt(Length);
        }
        public void insert(String item, int VNs){
            LinkedList<Integer> list = new LinkedList<>();
            for(int i=0;i<VNs;i++){
                 list.add(generateHash(item));
            }
            coord.put(item, list);
        }
        public void remove(String item){
            coord.remove(item);
        }
        public void change(String item, int VNs){
            if(VNs < LOWERBOUND) VNs = LOWERBOUND;
            if(VNs > UPPERBOUND) VNs = UPPERBOUND;
            containerVNs.put(item, VNs);
            LinkedList<Integer> list = coord.get(item);
            while(list.size()<VNs){
                list.add(generateHash(item));
            }
            while(list.size()>VNs){
                list.removeFirst();
            }
        }
        public int distance(String itemX, String itemY){
            LOG.info("Chord distance between "+itemX+"  "+itemY);
            LOG.info("Chord items "+coord.toString());
            int min = Length + 1000;
            LinkedList<Integer> x = coord.get(itemX), y = coord.get(itemY);
            for(int xx:x){
                for(int yy: y){
                    int t = xx-yy;
                    if(t<0)t = -t;
                    if(t < min){
                        min = t;
                    }
                }
            }
            return min;
        }
    };
    private class LocalityHashing{
        private long[] cost;
        private final int nlayer;
        private Map<String, ArrayList<String>> coord;
        public LocalityHashing(){
            nlayer = 5;
            cost = new long[nlayer];
            cost[nlayer - 1] = 1;
            for(int i= nlayer - 2 ;i >= 0;i--)cost[i] = cost[i+1] * 10;
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
            LOG.info("Locality distance between "+cid1+" "+cid2);
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
        //String containerHostUrl;
        public WebReader(){
            hostRackUrl = "http://192.168.0.36:8880";
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
    private ChordHashing chord;
    private LocalityHashing locality;
    private WebReader webReader;
    private Map<String,List<String>> hostRack = null;
    //private Map<String,String> containerHost = null;
    private Map<String, String> taskContainer = null;
    private Map<String, Integer> containerVNs; //number of VN for each container
    private Map<String, TaskModel> tasks; //Existing tasks
    private JobModel oldJobModel;
    private Config config;
    private final int defaultVNs;  // Default number of VNs for new coming containers
    private final double p1, p2;   // Weight parameter for Chord and Locality
    private UtilizationServer utilizationServer = null;
    private LocalityServer localityServer = null;
    private final int LOCALITY_RETRY_TIMES = 4;
    public MixedLocalityManager(){
        config = null;
        chord = new ChordHashing();
        locality = new LocalityHashing();
        webReader = new WebReader();
        taskContainer = new HashMap<>();
        //containerHost = new HashMap<>();
        hostRack = new HashMap<>();
        containerVNs = new HashMap<>();
        tasks = new HashMap<>();
        oldJobModel = null;
        defaultVNs = 100;
        p1 = 1;
        p2 = 0;
        utilizationServer = new UtilizationServer();
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
        int retry = LOCALITY_RETRY_TIMES;
        while(localityServer.getLocality(container) == null){
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
        containerVNs.put(container, defaultVNs);
        chord.insert(container, defaultVNs);
        locality.insert(container, getContainerLocality(container), 1);
    }
    // Container left
    private void removeContainer(String container){
        //TODO
        chord.remove(container);
        locality.remove(container);
        containerVNs.remove(container);
    }

    // Initial all tasks at the beginning;
    public void setTasks(Map<String, TaskModel> tasks){
        for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            chord.insert(task.getKey(), 1);
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
        LOG.info("Containers: "+containerVNs.toString());
        LOG.info("Tasks: "+tasks.toString());
        Map<String, LinkedList<TaskModel>> containerTasks = new HashMap<>();
        Map<String, ContainerModel> containers = new HashMap<>();
        for(String container:this.containerVNs.keySet()){
            String processor = container.substring(container.length()-6, container.length());
            //containers.put(processor, new ContainerModel(processor, 0, new HashMap<TaskName, TaskModel>()));
            containerTasks.put(processor, new LinkedList<>());
        }
        for(Map.Entry<String, TaskModel> task: tasks.entrySet()){
            //Find the closest container for each task
            String minContainer = null;
            double min = 0;
            for (String container: this.containerVNs.keySet()){
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
        for(String container:this.containerVNs.keySet()){
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
            if(!this.containerVNs.containsKey(processor)){
                insertContainer(processor);

            }
        }
        //Remove containers no longer exist
        for(String container: this.containerVNs.keySet()){
            if(!containers.contains(container)){
                removeContainer(container);
            }
        }
        return generateJobModel();
    }
    public JobModel rebalanceJobModel(){
        LOG.info("Rebalancing");
        HashMap util = utilizationServer.getAndRemoveUtilizationMap();
        LOG.info("Utilization map is: " + util.toString());
        return generateNewJobModel(util);
    }
    // Generate new job model when utilization changes.
    public JobModel generateNewJobModel(Map<String, Float> utlization){
        //TODO
        /*
        Calculate VNs according to utilization
        */
        for(Map.Entry<String,Float> entry: utlization.entrySet()){
            float usage = entry.getValue();
            //LOG.info("Utilization of " +entry.getKey()+" is: "+entry.getValue());
            if(usage<50.0)chord.change(entry.getKey(), (50-Math.round(usage))/25*defaultVNs + getCurrentVNs(entry.getKey()));
            else if(usage>80.0)chord.change(entry.getKey(), getCurrentVNs(entry.getKey()) - (Math.round(usage)-80)*defaultVNs/40);
        }
        LOG.info("New VNs: " + containerVNs.toString());
        return generateJobModel();
    }
    private int getCurrentVNs(String processorId){
        return this.containerVNs.get(processorId);
    }
    public double distance(String t1, String t2){
        return p1*chord.distance(t1,t2)+p2*locality.distance(t1,t2);
    }
    public double getUtil(String processorId){
        return utilizationServer.getUtilization(processorId);
    }
    public HashMap getUtilMap(){
        return utilizationServer.getAndRemoveUtilizationMap();
    }
}
