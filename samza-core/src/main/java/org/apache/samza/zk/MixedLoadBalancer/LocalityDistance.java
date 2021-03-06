package org.apache.samza.zk.MixedLoadBalancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalityDistance {
    private static final Logger LOG = LoggerFactory.getLogger(LocalityDistance.class);
    private long[] cost;
    private final int nlayer;
    private Map<String, ArrayList<String>> coord;
    private Map<String, ArrayList<String>> taskCoord; //position of task's partition

    public LocalityDistance() {
        nlayer = 5;
        cost = new long[nlayer];
        cost[0] = 1;
        for (int i = 1; i < nlayer; i++) cost[i] = cost[i - 1] * 10;
        coord = new HashMap<>();
    }

    // Id, Locality list (cluster, rack, server, container), Amount of state (1 for container)
    public void insert(String Id, List<String> items, Integer amount) {
        LOG.info("Inserting to locality list container " + Id + " :" + items.toString());
        if (coord.containsKey(Id)) {
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

    public void remove(String Id) {
        coord.remove(Id);
    }

    public long distance(String cid1, String cid2) {
        long sum = 0;
        LOG.info("Calculating Locality distance between " + cid1 + " " + cid2);
        LOG.info("Locality items " + coord.toString());
        ArrayList<String> v1 = coord.get(cid1), v2 = coord.get(cid2);
        LOG.info("Locality items " + v1.toString() + "      " + v2.toString());
        for (int i = 0; i < nlayer - 1; i++) {
            if (!v1.get(i).equals(v2.get(i))) {
                sum += cost[i];
            }
        }
        LOG.info("Distance " + sum);
        return sum * Integer.parseInt(v1.get(nlayer - 1)) * Integer.parseInt(v2.get(nlayer - 1));
    }

}
