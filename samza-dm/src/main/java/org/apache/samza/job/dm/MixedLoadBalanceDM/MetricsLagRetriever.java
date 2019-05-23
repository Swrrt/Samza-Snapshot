package org.apache.samza.job.dm.MixedLoadBalanceDM;

import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.config.Config;
import org.apache.samza.job.dm.MixedLoadBalancer.MixedLoadBalanceManager;
import org.apache.samza.job.dm.StageReport;
import org.apache.samza.zk.RMI.OffsetClient;
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
    private ConcurrentMap<String, Double> processingSpeed;
    private ConcurrentMap<Integer, Double> arrivalRate;
    private ConcurrentMap<Integer, Long> arrivalTime;
    private ConcurrentMap<String, Long> time, processed;
    private ConcurrentMap<Integer, Long> arrived;
    private ConcurrentMap<Integer, Long> backlog;
    private ConcurrentMap<Integer, Double> avgBacklog;
    private ConcurrentMap<String, Long> flushProcessed;
    private OffsetClient offsetClient = null;
    private HashMap<String, Long> beginOffset = null;
    private HashMap<String, Long> lastProcessedOffset = null;
    private long lastTimeUpdate = 0, lastProcessedInterval = 500;
    private final double delta = 7.0/8.0; //Parameter to smooth processing speed
    private final double arrivalDelta = 7.0/8.0;
    public void initial(String appName, String topic_name){
        topic = topic_name;
        app = appName;
        processingSpeed = new ConcurrentHashMap<>();
        arrivalRate = new ConcurrentHashMap<>();
        arrivalTime = new ConcurrentHashMap<>();
        backlog = new ConcurrentHashMap<>();
        time = new ConcurrentHashMap<>();
        processed = new ConcurrentHashMap<>();
        arrived = new ConcurrentHashMap<>();
        avgBacklog = new ConcurrentHashMap<>();
        flushProcessed = new ConcurrentHashMap<>();
    }
    public void setOffsetClient(Config config){
        this.offsetClient = new OffsetClient(
                config.get("job.loadbalance.offsetserver.address",""),
                Integer.parseInt(config.get("job.loadbalance.offsetserver.port","8884")),
                config.get("job.default.system"),
                config.get("job.loadbalance.inputtopic"),
                false
        );
    }
    //Update metrics from record
    public void update(ConsumerRecord<String, String> record){
        JSONObject json = new JSONObject(record.value());
        //writeLog("What happened: " + json);

        try {
            if (!isOurApp(json, app)) return;
            //writeLog("Our apps's record");
            String kafkaMetrics = json.getJSONObject("metrics").getJSONObject("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics").toString();
            if (kafkaMetrics != null) {
                //If KafkaSystemConsumerMetrics is here, we get lag information
                //writeLog("kafkaMetrics: " + kafkaMetrics);
                long time = json.getJSONObject("header").getLong("time");
                List<Integer> partitions = findPartitions(kafkaMetrics, topic);
                //if(partitions.size()>0) writeLog("Partitions: " + partitions);
                for (int partition : partitions) {
                    try{
                        updateArrived(partition, kafkaMetrics, time);
                    }catch (Exception e){
                        writeLog("Partition " + partition +" error: " + e.toString());
                    }
                }
            }
        }catch (Exception e) {
            //writeLog("Exception when read kafkaSystemConsumerMetrics: "+e);
        }
        /*try{
            if(!isOurApp(json, app)) return;
            updateFromTask(json);
        }catch (Exception e){
            writeLog(e.toString());
        }*/
        try{
            if (!isOurApp(json, app)) return;
            updateProcessed(json);
                //writeLog("TaskName: " + taskName + "   lastTime: " + lastTime + " lastProcessed: " + lastProcessed + " lastSpeed: " + lastSpeed + " delta: " +delta);
                //writeLog("TaskName: " + taskName + "   Time: " + currentTime + " Processed: " + currentProcessed + " Speed: " + newSpeed);
        }catch (Exception e){
            //writeLog("Error when parse taskMetrics: "+ e);
        }

        //For validation
        try{
            if(!isOurApp(json, app)) return ;
            JSONObject containerMetrics = json.getJSONObject("metrics").getJSONObject("org.apache.samza.container.SamzaContainerMetrics");
            if(containerMetrics != null){
                String containerId = json.getJSONObject("header").getString("source");
                containerId = containerId.substring(containerId.length() - 6);
                long processed = containerMetrics.getLong("process-envelopes");
                flushProcessed.put(containerId, processed);
                System.out.println("MixedLoadBalanceManager, time " + json.getJSONObject("header").getLong("time") +" : " + "Flush Processed: " + flushProcessed);
                flushProcessed.clear();
            }
        }catch (Exception e){

        }
    }

    private int getPartition(JSONObject json)throws Exception{
        String taskString = json.getJSONObject("header").getString("source");
        int i = taskString.indexOf("Partition");
        if(i == -1)throw new Exception("Not task instance exception");
        int partition = Integer.valueOf(taskString.substring(i + 10));
        return partition;
    }

    private long getArrived(JSONObject taskMetrics, int partition){
        return Long.valueOf(taskMetrics.getString("kafka-" + topic.toLowerCase() + "-" +partition + "-offset"));
    }

    private long getProcessed(JSONObject taskMetrics){
        return taskMetrics.getLong("messages-actually-processed");
    }

    /*
       Get processed, arrived from TaskInstanceMetrics
       Update processing speed, arrival rate and backlog accordingly

       if offset=null, return nothing

       !!!!!Buggy!!!!!
     */
    /*private void updateFromTask(JSONObject json)throws Exception{
        JSONObject taskMetrics = json.getJSONObject("metrics").getJSONObject("org.apache.samza.container.TaskInstanceMetrics");
        int partition = getPartition(json);

        String taskName = json.getJSONObject("header").getString("source");
        taskName = taskName.substring(taskName.indexOf("TaskName-") + 9);

        long currentArrived = getArrived(taskMetrics, partition);
        long currentProcessed = getProcessed(taskMetrics);
        long currentBacklog = currentArrived - currentProcessed;
        long currentTime = json.getJSONObject("header").getLong("time");

        backlog.put(partition, currentBacklog);
        double lastLag = 0;
        if(avgBacklog.containsKey(partition)){
            lastLag = avgBacklog.get(partition);
        }
        avgBacklog.put(partition, (arrivalDelta)*lastLag + (1 - arrivalDelta) * currentBacklog);

        long lastArrived = 0;
        if(arrived.containsKey(partition)){
            lastArrived = arrived.get(partition);
        }
        if(lastArrived > currentArrived){
            return ;
        }
        double arrivedInPeriod = currentArrived - lastArrived;
        arrived.put(partition, currentArrived);

        double lastArrivedRate = 0;
        if(arrivalRate.containsKey(partition)){
            lastArrivedRate = arrivalRate.get(partition);
        }
        long lastTime = 0;
        if(arrivalTime.containsKey(partition)){
            lastTime = arrivalTime.get(partition);
        }
        arrivalTime.put(partition, currentTime);

        double newArrival = lastArrivedRate;
        if(currentTime > lastTime){
            newArrival = arrivalDelta * lastArrivedRate + (1 - arrivalDelta) * (arrivedInPeriod) * 1000/ (currentTime - lastTime);
        }
        arrivalRate.put(partition, newArrival);

        long lastProcessed = 0;
        lastTime = 0;
        if (processed.containsKey(taskName)) {
            lastProcessed = processed.get(taskName);
            lastTime = time.get(taskName);
        }
        time.put(taskName, currentTime);

        processed.put(taskName, currentProcessed);

        double lastSpeed = 0;
        if (processingSpeed.containsKey(taskName)) {
            lastSpeed = processingSpeed.get(taskName);
        }

        double newSpeed = lastSpeed;
        if (currentTime > lastTime) {
            newSpeed = delta * lastSpeed + (1 - delta) * ((double) currentProcessed - lastProcessed) * 1000 / (currentTime - lastTime); // 1000 since it's millisecond
        }
        if (newSpeed > -1e-9) {
            processingSpeed.put(taskName, newSpeed);
        }
    }*/

    private String partitionToTaskName(int partition){
        return "Partition "+partition;
    }
    private int taskNameToPartition(String taskName){
        return Integer.valueOf(taskName.substring(10)); //Skip 'Partition '
    }
    private void updateBacklog(int partition, long currentBacklog){
        if(currentBacklog < 0) currentBacklog = 0;
        backlog.put(partition, currentBacklog);
        double lastLag = 0;
        if(avgBacklog.containsKey(partition)){
            lastLag = avgBacklog.get(partition);
        }
        avgBacklog.put(partition, (arrivalDelta)*lastLag + (1 - arrivalDelta) * currentBacklog);
    }

    private void updateProcessed(JSONObject json){
        //If TaskInstanceMetrics is here, we get processing speed
        //writeLog("taskMetrics: " + taskMetrics);
        JSONObject taskMetrics = json.getJSONObject("metrics").getJSONObject("org.apache.samza.container.TaskInstanceMetrics");
        if (taskMetrics != null) {
            String taskName = json.getJSONObject("header").getString("source");
            //Need to get correct Task name
            taskName = taskName.substring(taskName.indexOf("TaskName-") + 9);

            long currentTime = json.getJSONObject("header").getLong("time");
            long currentProcessed = taskMetrics.getLong("messages-total-processed") - getBegin(taskNameToPartition(taskName)); //taskMetrics.getLong("messages-actually-processed") + getLastProcessed(taskNameToPartition(taskName)) - getBegin(taskNameToPartition(taskName));

            long lastProcessed = 0, lastTime = 0;
            if (processed.containsKey(taskName)) {
                lastProcessed = processed.get(taskName);
                lastTime = time.get(taskName);
            }
            time.put(taskName, currentTime);


            processed.put(taskName, currentProcessed);
            if(arrived.containsKey(taskNameToPartition(taskName))){
                updateBacklog(taskNameToPartition(taskName),  arrived.get(taskNameToPartition(taskName)) - currentProcessed);
            }

            double lastSpeed = 0;
            if (processingSpeed.containsKey(taskName)) {
                lastSpeed = processingSpeed.get(taskName);
            }

            double newSpeed = lastSpeed;
            if (currentTime > lastTime) {
                newSpeed = delta * lastSpeed + (1 - delta) * ((double) currentProcessed - lastProcessed) * 1000 / (currentTime - lastTime); // 1000 since it's millisecond
            }
            if (newSpeed > -1e-9) {
                processingSpeed.put(taskName, newSpeed);
            }
        }
    }

    // # of messages read
    private long getRead(int partition, String kafkaMetric){
        String pattern = "kafka-"+topic.toLowerCase()+"-"+partition+"-messages-read\":";
        int i = kafkaMetric.indexOf(pattern);
        int j = kafkaMetric.indexOf(',', i);
        return Long.valueOf(kafkaMetric.substring(i+pattern.length(), j));
    }

    private long getLag(int partition, String kafkaMetric){
        String pattern = "kafka-"+topic.toLowerCase()+"-"+partition+"-messages-behind-high-watermark\":";
        int i = kafkaMetric.indexOf(pattern);
        int j = kafkaMetric.indexOf(',', i);
        return Long.valueOf(kafkaMetric.substring(i+pattern.length(), j));
    }

    private long getWatermark(int partition, String kafkaMetric){
        String pattern = "kafka-"+topic.toLowerCase()+"-"+partition+"-high-watermark\":";
        int i = kafkaMetric.indexOf(pattern);
        int j = kafkaMetric.indexOf(',', i);
        return Long.valueOf(kafkaMetric.substring(i+pattern.length(), j));
    }
    private long getBegin(int partition){
        if(beginOffset == null || !beginOffset.containsKey(String.valueOf("Partition " + partition)))beginOffset = offsetClient.getBeginOffset();
        return beginOffset.get(String.valueOf("Partition " + partition));
    }

    private long getLastProcessed(int partition){
        if(lastProcessedOffset == null || System.currentTimeMillis() - lastTimeUpdate > lastProcessedInterval) {
            lastTimeUpdate = System.currentTimeMillis();
            lastProcessedOffset = offsetClient.getProcessedOffset();
        }
        return lastProcessedOffset.get("Partition " + partition);
    }

    private void updateArrived(int partition, String kafkaMetric, long time){
        /*long lag = getLag(partition, kafkaMetric);
        long fetched = getRead(partition, kafkaMetric);*/
        long head = getBegin(partition);
        long watermark = getWatermark(partition, kafkaMetric);
        if(watermark < 0){ //not ready
            return ;
        }
        long lastArrived = 0;
        if(arrived.containsKey(partition)){
            lastArrived = arrived.get(partition);
        }
        /*// After rebalance, the lag and fetched will start from 0 again.
        if(lastArrived > lag + fetched){
            lastArrived = 0;
            arrivalRate.put(partition, 0.0);
        }*/

        double arrivedInPeriod = watermark - head - lastArrived;
        arrived.put(partition, watermark - head);
        if(processed.containsKey(partitionToTaskName(partition))){
            updateBacklog(partition, (watermark - head) - processed.get(partitionToTaskName(partition)));
        };

        double lastArrivedRate = 0;
        if(arrivalRate.containsKey(partition)){
            lastArrivedRate = arrivalRate.get(partition);
        }
        long lastTime = 0;
        if(arrivalTime.containsKey(partition)){
            lastTime = arrivalTime.get(partition);
        }
        arrivalTime.put(partition, time);

        double newArrival = lastArrivedRate;
        if(time > lastTime){
            newArrival = arrivalDelta * lastArrivedRate + (1 - arrivalDelta) * (arrivedInPeriod) * 1000/ (time - lastTime);
        }
        arrivalRate.put(partition, newArrival);
    }

    //Use metric like this: 'blocking-poll-count-SystemStreamPartition [kafka, StreamBenchInput, 0]
    //To find all partitions in the metric record.
    private List<Integer> findPartitions(String string, String topic){
        List<Integer> partitions = new LinkedList<>();
        String pattern = "[kafka, " + topic +", ";
        int i=0, len = pattern.length();
        //Find all patterns in string
        //writeLog("Kafka metrics: " + string);
        while(i != -1){
            i = string.indexOf(pattern, i);
            //writeLog("Find pattern at: "+ i);
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

    public Map<Integer, Long> retrieveArrived(){
        return arrived;
    }
    public Map<Integer, Long> retrieveArrivedTime(){
        return arrivalTime;
    }

    public Map<String, Long> retrieveFlushProcessed(){
        Map<String, Long> ret = new HashMap<>();
        ret.putAll(flushProcessed);
        flushProcessed.clear();
        return ret;
    }


    public Map<String, Long> retrieveProcessed(){
        return processed;
    }

    public Map<Integer, Double> retrieveAvgBacklog(){
        //writeLog("Retrieving average backlog information: " + avgBacklog.toString());
        return avgBacklog;
    }

    public Map<Integer, Double> retrieveArrivalRate(){
        //writeLog("Retrieved arrival rate information: " + arrivalRate.toString());
        return arrivalRate;
    }

    //Asynchronous access
    public Map<Integer, Long> retrieveBacklog(){
        writeLog("Retrieved backlog information: " + backlog.toString());
        return backlog;
    }

    // Access Kafka server
    // Return a containerId-processSpeed map
    public Map<String, Double> retrieveProcessingSpeed(){
        //writeLog("Retrieved speed information: " + processingSpeed.toString());
        return processingSpeed;
    }

    //Need flush metrics after rebalancing
    /*
    public void flush(){
        processingSpeed.clear();
        arrivalRate.clear();
        arrivalTime.clear();
        backlog.clear();
        time.clear();
        processed.clear();
        arrived.clear();
        avgBacklog.clear();
    }*/
    private void writeLog(String log){
        System.out.println("MetricsLagRetriever: " + log);
    }
}
