package org.apache.samza.job.dm.MixedLoadBalanceDM;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.config.Config;
import org.apache.samza.job.dm.StageReport;
import org.json.JSONObject;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/*
    Retrieve backlog information from metrics.
    Asynchronous retrieve data and report
*/
public class MetricsLagRetriever {
    //private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetRetriever.class);
    private Properties properties;
    private String topic, app;
    private ConcurrentMap<String, Double> speed;
    private ConcurrentMap<String, Long> time, processed;
    private ConcurrentMap<Integer, Long> backlog;
    private double delta = 0.5; //Parameter to smooth processing speed
    public void initial(String topic_name, String appName){
        topic = topic_name;
        app = appName;
        speed = new ConcurrentHashMap<>();
        backlog = new ConcurrentHashMap<>();
        time = new ConcurrentHashMap<>();
        processed = new ConcurrentHashMap<>();
    }
    //Update metrics from record
    public void update(ConsumerRecord<String, String> record){
        JSONObject json = new JSONObject(record.value());

        try {
            if (!isOurApp(json, app)) return;
            writeLog("Our apps's record");
            String kafkaMetrics = json.getJSONObject("metrics").getString("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics");
            JSONObject taskMetrics = json.getJSONObject("metrics").getJSONObject("org.apache.samza.system.kafka.TaskInstanceMetrics");

            //If KafkaSystemConsumerMetrics is here, we get lag information
            if (kafkaMetrics != null) {
                writeLog("kafkaMetrics: " + kafkaMetrics);
                List<Integer> partitions = findPartitions(kafkaMetrics, topic);
                writeLog("Partitions: " + partitions);
                for (int partition : partitions) {
                    updateBacklog(partition, kafkaMetrics);
                }
            }

            //If TaskInstanceMetrics is here, we get processing speed
            if (taskMetrics != null) {
                writeLog("taskMetrics: " + taskMetrics);
                String taskName = json.getJSONObject("header").getString("source");
                //Need to get correct Task name
                taskName = taskName.substring(taskName.indexOf("TaskName-") + 9);

                long currentTime = json.getJSONObject("header").getLong("time");
                long currentProcessed = taskMetrics.getLong("messages-actually-processed");

                long lastProcessed = 0, lastTime = 0;
                if (processed.containsKey(taskName)) {
                    lastProcessed = processed.get(taskName);
                    lastTime = time.get(taskName);
                }
                time.put(taskName, currentTime);
                processed.put(taskName, currentProcessed);

                double lastSpeed = 0;
                if (speed.containsKey(taskName)) {
                    lastSpeed = speed.get(taskName);
                }

                double newSpeed = delta * lastSpeed;
                if (currentTime > lastTime) {
                    newSpeed += (1 - delta) * ((double) currentProcessed - lastProcessed) / (currentTime - lastTime);
                }
                if (newSpeed > -1e-9) {
                    speed.put(taskName, newSpeed);
                }
            }
        }catch (Exception e){
            writeLog("Error when parse metrics: "+ e.toString());
        }
    }

    private long getLag(int partition, String kafkaMetric){
        String pattern = "\"kafka-"+topic.toLowerCase()+"-"+partition+"messages-behind-high-watermark\":";
        int i = kafkaMetric.indexOf(pattern);
        int j = kafkaMetric.indexOf(',', i);
        return Long.valueOf(kafkaMetric.substring(i+pattern.length(), j));
    }

    private void updateBacklog(int partition, String kafkaMetric){
        long lag = getLag(partition, kafkaMetric);
        backlog.put(partition, lag);
    }

    //Use metric like this: 'blocking-poll-count-SystemStreamPartition [kafka, StreamBenchInput, 0]
    //To find all partitions in the metric record.
    private List<Integer> findPartitions(String string, String topic){
        List<Integer> partitions = new LinkedList<>();
        String pattern = "[kafka, " + topic +", ";
        int i=0, len = pattern.length();
        //Find all patterns in string
        while(i != -1){
            i = string.indexOf(pattern, i);
            if(i != -1){
                int j = string.indexOf(']', i + len); //Right bracket
                partitions.add(Integer.valueOf(string.substring(i + len, j)));
                i++;
            }
        }
        return partitions;
    }

    private boolean isOurApp(JSONObject json, String appName){
        try{
            return json.getJSONObject("header").getString("job-name").equals(appName);
        }catch (Exception e){
        }
        return false;
    }

    private boolean isOurTopic(JSONObject json, String topicName){
        try {
            String string = json.getJSONObject("metrics").getString("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics");
            if(string.contains(" "+topicName+","))return true;
        }catch (Exception e){}
        return false;
    }

    //Asynchronous access
    public Map<Integer, Long> retrieveBacklog(){
        writeLog("Retrieving backlog from Kafka console");
        writeLog("Retrieved backlog information: " + backlog.toString());
        return backlog;
    }
    // Access Kafka server
    // Return a containerId-processSpeed map
    public Map<String, Double> retrieveSpeed(){
        writeLog("Retrieving speed information from Kafka console");
        writeLog("Retrieved speed information: " + speed.toString());
        return speed;
    }
    private void writeLog(String log){
        System.out.println("MetricsLagRetriever: " + log);
    }
}
