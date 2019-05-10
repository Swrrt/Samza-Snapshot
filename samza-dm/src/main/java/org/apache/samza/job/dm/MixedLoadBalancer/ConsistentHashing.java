package org.apache.samza.job.dm.MixedLoadBalancer;

import com.google.common.hash.Hashing;
import org.apache.commons.io.Charsets;
import org.apache.samza.job.model.TaskModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ConsistentHashing {
    //private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashing.class);
    private int Length;
    private Map<String, LinkedList<Integer>> coord;
    private Map<String, Integer> taskCoord;

    public ConsistentHashing() {
        coord = new HashMap<>();
    }

    // Shuffle tasks to the ring
    protected void initTasks(Map<String, TaskModel> tasks) {
        Length = tasks.size();
        List<String> taskNames = new ArrayList<>(Length);
        for (Map.Entry<String, TaskModel> task : tasks.entrySet()) {
            taskNames.add(task.getKey());
        }
        Collections.shuffle(taskNames);
        taskCoord = new HashMap<>();
        for (int i = 0; i < Length; i++) {
            taskCoord.put(taskNames.get(i), i);
        }
    }

    // Generate hash value for strings
    private int generateHash(String item) {
        //Using SHA-1 Hashing
        int hash = Hashing.sha1().hashString(item, Charsets.UTF_8).asInt() % Length; //Could be negative
        if(hash < 0) hash += Length;
        return hash;
    }

    //Generate Virtual Node id
    private String generateVNName(String containerId, int VNid) {
        return containerId + '_' + VNid;
    }

    public void insert(String item, int VNs) {
        LinkedList<Integer> list = new LinkedList<>();
        for (int i = 0; i < VNs; i++) {
            list.add(generateHash(generateVNName(item, i)));
        }
        coord.put(item, list);
    }

    public void remove(String item) {
        coord.remove(item);
    }
    public int getVNnumbers(String containerId){
        return coord.get(containerId).size();
    }
    /*
        Add a VN to container
     */
    public void addVN(String containerId, int vn) {
        writeLog("Add Virtual Node at position: " + vn + " to container " + containerId);
        coord.get(containerId).add(vn);
    }

    /*
        Remove a VN from container.
     */
    public int removeVN(String containerId) {
        writeLog("Remove Virtual Node from to container " + containerId);
        int randomNum = ThreadLocalRandom.current().nextInt(0, coord.get(containerId).size());
        return coord.get(containerId).remove(randomNum);
    }

    public void simpleBalance(String maxContainer, String minContainer) {
        writeLog("Move virtual node of " + maxContainer + " to " + minContainer);
        if (coord.get(maxContainer).size() > 1) {
            int vn = removeVN(maxContainer);
            writeLog("Move the VN at position: " + vn);
            addVN(minContainer, vn);
        } else writeLog(maxContainer + " only has 1 VN. No movement");

    }

    public int distance(String itemX, String itemY) {
        //writeLog("Calculate load distance between " + itemX + "  " + itemY);
        //writeLog("load items " + coord.toString());
        int min = Length + 1000;
        int x = taskCoord.get(itemX);
        LinkedList<Integer> y = coord.get(itemY);
        for (int yy : y) {
            int t = x - yy;
            if (t < 0) t = -t;
            if (t < min) {
                min = t;
            }
        }
        //writeLog("Calculate load distance between " + itemX + "  " + itemY + " is: " + min);
        return min;
    }
    private void writeLog(String log){
        System.out.println("ConsistentHashing: " + log);
    }
}
