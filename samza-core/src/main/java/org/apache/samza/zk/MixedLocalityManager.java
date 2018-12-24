package org.apache.samza.zk;

import javafx.util.Pair;
import org.apache.samza.container.LocalityManager;

import java.util.*;

import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.json.*;
import org.apache.commons.io.*;
import java.net.URL;
import java.nio.charset.*;

public class MixedLocalityManager {
    private class ChordHashing{
        private final int Length;
        private Map<String, ArrayList<Integer>> coord;
        public ChordHashing(){
            Length = 100000007;
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
            ArrayList<Integer> list = new ArrayList<>();
            for(int i=0;i<VNs;i++){
                 list.add(generateHash(item));
            }
            coord.put(item, list);
        }
        public void remove(String item){
            coord.remove(item);
        }
        public int distance(String itemX, String itemY){
            int min = Length + 1000;
            ArrayList<Integer> x = coord.get(itemX), y = coord.get(itemY);
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
            if(coord.containsKey(Id)){
               coord.remove(Id);
            }
            ArrayList<String> value = new ArrayList<>();
            int i = 0;
            for (String item : items) {
                value.add(item);
            }
            value.add(amount.toString());
        }
        public void remove(String Id){
            coord.remove(Id);
        }
        public long distance(String cid1, String cid2){
            long sum = 0;
            ArrayList<String> v1 = coord.get(cid1), v2 = coord.get(cid2);
            for(int i=0 ; i < nlayer; i++){
                if(!v1.get(i).equals(v2.get(i))){
                    sum += cost[i];
                }
            }
            return sum * Integer.getInteger(v1.get(nlayer)) * Integer.getInteger(v2.get(nlayer));
        }
    }
    private class WebReader{
        String hostRackUrl;
        String containerHostUrl;
        public WebReader(){
            hostRackUrl = "192.168.0.36:8001";
            containerHostUrl = "192.168.0.36:8002";
        }
        public WebReader(String s1, String s2){
            hostRackUrl = new String(s1);
            containerHostUrl = new String(s2);
        }
        public Map<String, List<String>> readHostRack() {
            Map<String, List<String>> hostRack = new HashMap<>();
            try{
                JSONObject json = new JSONObject(IOUtils.toString(new URL(hostRackUrl), Charset.forName("UTF-8")));
                for(Object key: json.keySet()){
                    String keyStr = (String)key;
                    String value = json.getString(keyStr);
                    if(!hostRack.containsKey(keyStr)) {
                        hostRack.put(keyStr, new LinkedList<>());
                    }
                    hostRack.get(keyStr).add(value);
                }
            }catch(Exception e){
            }
            return hostRack;
        }
        public Map<String, String> readContainerHost(){
            Map<String, String> containerHost = new HashMap<>();
            try{
                JSONObject json = new JSONObject(IOUtils.toString(new URL(containerHostUrl), Charset.forName("UTF-8")));
                for(Object key: json.keySet()){
                    String keyStr = (String)key;
                    String value = json.getString(keyStr);
                    containerHost.put(keyStr,value);
                }
            }catch(Exception e){
            }
            return containerHost;
        }

    }
    private ChordHashing chord;
    private LocalityHashing locality;
    private WebReader webReader;
    private Map<String,List<String>> hostRack = null;
    private Map<String,String> containerHost = null;
    private Map<String, String> taskContainer = null;
    private Map<String, Integer> containers; //number of VN for each container
    private Set<String> tasks; //Existing tasks
    private Map<String, String> processorIdToContainer = null;
    private JobModel oldJobModel;
    private final int defaultVNs;  // Default number of VNs for new coming containers
    private final double p1, p2;   // Weight parameter for Chord and Locality
    public MixedLocalityManager(){
        chord = new ChordHashing();
        locality = new LocalityHashing();
        webReader = new WebReader();
        taskContainer = new HashMap<>();
        containerHost = new HashMap<>();
        containers = new HashMap<>();
        tasks = new HashSet<>();
        processorIdToContainer = new HashMap<>();
        oldJobModel = null;
        defaultVNs = 100;
        p1 = 1;
        p2 = 0;
    }
    public void initial(JobModel jobModel){
        getHostRack();
        oldJobModel = jobModel;
    }
    // Read container-host mapping from web
    private Map<String, String> getContainerHost(){
        return webReader.readContainerHost();
    }
    // Read host-rack-cluster mapping from web
    private Map<String, List<String>> getHostRack(){
        if(hostRack == null){
            hostRack = webReader.readHostRack();
        }
        return hostRack;
    }
    // Construct the container-(container, host, rack cluster) mapping
    private List<String> getContainerLocality(String item){
        //TODO
        getContainerHost();
        return null;
    }
    // Construct the task-(container, host, rack cluster) mapping
    private List<String> getTaskLocality(String item){
        //TODO

        return null;
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
    public void insertContainer(String container){
        //TODO
        containers.put(container, defaultVNs);
        chord.insert(container, defaultVNs);
        locality.insert(container, getContainerLocality(container), 1);
    }
    // Container left
    public void removeContainer(String container){
        //TODO
        chord.remove(container);
        locality.remove(container);
        containers.remove(container);
    }

    // Initial all tasks at the beginning;
    public void setTasks(List<String> tasks){
        for(String task: tasks){
            this.tasks.add(task);
            chord.insert(task, 1);
            locality.insert(task, getTaskLocality(task), 1);
        }
    }
    private String getContainerID(String processor){
        //TODO
        //Translate processor ID to Container ID;
        return null;
    }
    public JobModel generateJobModel(){
        //TODO
        //generate new job model from current containers and tasks setting
        //store the new job model for future use;

        return oldJobModel;
    }
    // Generate new Job Model based on new processors list
    public JobModel generateNewJobModel(List<String> processors){
        Set<String> containers = new HashSet<>();
        //Translate from processorID to container ID
        for(String processor: processors){
            String container = getContainerID(processor);
            containers.add(container);
            //Insert new container
            if(!this.containers.containsKey(container)){
                insertContainer(container);
            }
        }
        //Remove containers no longer exist
        for(String container: this.containers.keySet()){
            if(!containers.contains(container)){
                removeContainer(container);
            }
        }
        return generateJobModel();
    }
    public double distance(String t1, String t2){
        return p1*chord.distance(t1,t2)+p2*locality.distance(t1,t2);
    }
}
