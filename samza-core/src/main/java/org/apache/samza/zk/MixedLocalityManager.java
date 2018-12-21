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
    ChordHashing chord;
    LocalityHashing locality;
    WebReader webReader;
    Map<String,List<String>> hostRack = null;
    Map<String,String> containerHost = null;
    Map<String, String> taskContainer = null;
    Map<String, Integer> containers; //number of VN
    Set<String> tasks;
    final double p1, p2;
    public MixedLocalityManager(){
        chord = new ChordHashing();
        locality = new LocalityHashing();
        webReader = new WebReader();
        taskContainer = new HashMap<>();
        containerHost = new HashMap<>();
        containers = new HashMap<>();
        tasks = new HashSet<>();
        p1 = 0.5;
        p2 = 0.5;
    }
    public MixedLocalityManager(double pp1, double pp2){
        chord = new ChordHashing();
        locality = new LocalityHashing();
        webReader = new WebReader();
        taskContainer = new HashMap<>();
        containerHost = new HashMap<>();
        containers = new HashMap<>();
        tasks = new HashSet<>();
        p1 = pp1;
        p2 = pp2;
    }
    private Map<String, String> getLocality(){
        return webReader.readContainerHost();
    }
    private Map<String, List<String>> getHostRack(){
        if(hostRack == null){
            hostRack = webReader.readHostRack();
        }
        return hostRack;
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
    public void updateContainers(){
        for(String containerId: containers.keySet()){
            if(!containerHost.containsKey(containerId)){
                chord.remove(containerId);
                locality.remove(containerId);
            }
        }
        for(Map.Entry<String, String> containerId: containerHost.entrySet()){
            if(containers.containsKey(containerId.getKey())){
                chord.
            }else{
                chord.insert(containerId, 5);
                String host = containerHost.get(containerId);
                List<String> local = (List)((ArrayList)hostRack.get(host)).clone();
                local.add(host);
                locality.insert(containerId, local, 10);
            }
        }
    }
    public void updateTasks(){

    }
    public void update(JobModel jobModel){
        containerHost = webReader.readContainerHost();
        hostRack = webReader.readHostRack();
        taskContainer = getTaskContainer(jobModel);
        updateContainers();
        updateTasks();
    }
    public double distance(String t1, String t2){
        return p1*chord.distance(t1,t2)+p2*locality.distance(t1,t2);
    }
}
