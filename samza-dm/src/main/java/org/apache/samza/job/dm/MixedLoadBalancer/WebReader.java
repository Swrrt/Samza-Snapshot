package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class WebReader {
    private static final Logger LOG = LoggerFactory.getLogger(WebReader.class);
    String hostRackUrl;
    String hostHostUrl;

    //String containerHostUrl;
    public WebReader() {
        hostRackUrl = "http://192.168.0.36:8880";
        hostHostUrl = "http://192.168.0.36:8880";
        //containerHostUrl = "http://192.168.0.36:8881";
    }

    public WebReader(String s1, String s2) {
        hostRackUrl = new String(s1);
        //containerHostUrl = new String(s2);
    }

    public Map<String, List<String>> readHostRack() {
        Map<String, List<String>> hostRack = new HashMap<>();
        try {
            LOG.info("Reading Host-Rack information from ".concat(hostRackUrl));
            JSONObject json = new JSONObject(IOUtils.toString(new URL(hostRackUrl), Charset.forName("UTF-8")));
            LOG.info("Host-rack information ".concat(json.toString()));
            for (Object key : json.keySet()) {
                String keyStr = (String) key;
                JSONArray value = json.getJSONArray(keyStr);
                if (!hostRack.containsKey(keyStr)) {
                    hostRack.put(keyStr, new LinkedList<>());
                }
                for (Object o : value) {
                    hostRack.get(keyStr).add((String) o);
                }
            }
        } catch (Exception e) {
            LOG.info("Error when reading Host-Rack information: " + e.toString());
        }
        return hostRack;
    }

    public Map<String, Map<String, Integer>> readHostHostDistance() {
        Map<String, Map<String, Integer>> hostHostDistance = new HashMap<>();
        try {
            LOG.info("Reading Host-Host information from ".concat(hostHostUrl));
            JSONObject json = new JSONObject(IOUtils.toString(new URL(hostHostUrl), Charset.forName("UTF-8")));
            LOG.debug("Host-Host information ".concat(json.toString()));
            for (Object key : json.keySet()) {
                String keyStr = (String) key;
                JSONObject values = json.getJSONObject(keyStr);
                if (!hostHostDistance.containsKey(keyStr)) {
                    hostHostDistance.put(keyStr, new HashMap<>());
                }
                for (Object value : values.keySet()) {
                    hostHostDistance.get(keyStr).put((String) value, values.getInt((String) value));
                }
            }
        } catch (Exception e) {
            LOG.info("Error when reading Host-Host information: " + e.toString());
        }
        return hostHostDistance;
    }

    public Map<String, String> readPartitionLeaderDistance() {
        Map<String, String> taskHostDistance = new HashMap<>();
        try {
            LOG.info("Reading PartitionLeader-Host information from ".concat(hostHostUrl));
            JSONObject json = new JSONObject(IOUtils.toString(new URL(hostHostUrl), Charset.forName("UTF-8")));
            LOG.debug("PartitionLeader-Host information ".concat(json.toString()));
            for (Object key : json.keySet()) {
                String keyStr = (String) key;
                String value = json.getString(keyStr);
                taskHostDistance.put(keyStr, value);
            }
        } catch (Exception e) {
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