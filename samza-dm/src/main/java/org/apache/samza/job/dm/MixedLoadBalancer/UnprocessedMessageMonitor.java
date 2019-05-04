package org.apache.samza.job.dm.MixedLoadBalancer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

//TODO: use Kafka information (offset) to monitor processing speed
public class UnprocessedMessageMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(UnprocessedMessageMonitor.class);
    private KafkaConsumer<String,String> consumer;
    private String topic;
    private ExecutorService executor;
    private long delay;
    private String appName;
    private final long processSpeedTimeout = 30000;
    private ConcurrentHashMap<String, Long> unprocessedMessages;
    private ConcurrentHashMap<String, Long> processedEnv;
    private ConcurrentHashMap<String, Double> processingSpeed;
    private ConcurrentHashMap<String, Long> processedTime;
    private Thread t;
    private final int MonitorSleepInterval = 1000;
    public UnprocessedMessageMonitor(){
        unprocessedMessages = new ConcurrentHashMap<>();
        processedEnv = new ConcurrentHashMap<>();
        processingSpeed = new ConcurrentHashMap<>();
        processedTime = new ConcurrentHashMap<>();
    }
    public void init(String brokers, String topic, String appName) {
        Properties props = createConsumerConfig(brokers, appName);
        this.appName = appName;
        //LOG.info("The App Name is: "+appName);
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }
    public void shutdown() {
        if (consumer != null)
            consumer.close();
        if (executor != null)
            executor.shutdown();
    }
    public void removeContainer(String containerId){
        LOG.info("Remove container: "+containerId);
        if(unprocessedMessages.contains(containerId)){
            unprocessedMessages.remove(containerId);
        }else LOG.info("Does not contain container: "+containerId);
        if(processedEnv.contains(containerId)){
            processedEnv.remove(containerId);
            processedTime.remove(containerId);
        }
        if(processingSpeed.contains(containerId)){
            processingSpeed.remove(containerId);
        }
    }
    public long getUnprocessedMessage(String containerId){
        return unprocessedMessages.get(containerId);
    }
    public HashMap<String, Long> getUnprocessedMessage(){
        HashMap<String, Long> ret = new HashMap<>();
        ret.putAll(unprocessedMessages);
        return ret;
    }
    public HashMap<String, Double> getProcessingSpeed(){
        HashMap<String, Double> ret = new HashMap<>();
        ret.putAll(processingSpeed);
        return ret;
    }
    private void run() {
        LOG.info("start running!");
        consumer.subscribe(Collections.singletonList(this.topic));
        while(true){
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    // sent kafka msg by http
                    // System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                    //LOG.info("Received metrics:"+record);
                    parseProcessEnvelopes(record.value());
                    parseUnprocessedMessages(record.value());
                }
                try{
                    Thread.sleep(MonitorSleepInterval);
                }catch (Exception e){
                }
            }catch(Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    private void parseProcessEnvelopes(String record) {
        JSONObject json = new JSONObject(record);
        //System.out.println(json);
        if (json.getJSONObject("header").getString("job-name").equals(appName) && json.getJSONObject("header").getString("container-name").contains("samza-container")) {
            //System.out.println("!!!!!!\n"+json.getJSONObject("metrics")+"!!!!!!\n");
            if (json.getJSONObject("metrics").has("org.apache.samza.container.SamzaContainerMetrics")) {
                long processEnvelopes = json.getJSONObject("metrics").
                        getJSONObject("org.apache.samza.container.SamzaContainerMetrics").
                        getLong("process-envelopes");
                long time = json.getJSONObject("header").getLong("time");
                String containerId = json.getJSONObject("header").getString("container-name");
                long dEnv = processEnvelopes;
                long dTime = time;
                if(processedTime.containsKey(containerId) && time - processedTime.get(containerId) > processSpeedTimeout){
                    processedTime.remove(containerId);
                    processedEnv.remove(containerId);
                    processingSpeed.remove(containerId);
                }
                if (processedEnv.containsKey(containerId) && processedTime.containsKey(containerId)) {
                    if (processEnvelopes > processedEnv.get(containerId)) {
                        dEnv -= processedEnv.get(containerId);
                        dTime -= processedTime.get(containerId);
                        double throughput = 0;
                        if (dTime > 0) {
                            throughput = dEnv / ((double) dTime);
                            processingSpeed.put(containerId, throughput * 1000);
                        }
                    }
                }
                processedEnv.put(containerId, processEnvelopes);
                processedTime.put(containerId, time);
            }
        }
    }
    private void parseUnprocessedMessages(String record){
        JSONObject json = new JSONObject(record);
        //System.out.println(json);
        //LOG.info("Json: "+json.toString());
        if(json.getJSONObject("header").getString("job-name").equals(appName) && json.getJSONObject("header").getString("container-name").contains("samza-container")){
            //LOG.info("!!!!\n"+json.getJSONObject("metrics")+"!!!!\n");
            //System.out.println("!!!!!!\n"+json.getJSONObject("metrics")+"!!!!!!\n");
            if(json.getJSONObject("metrics").has("org.apache.samza.system.SystemConsumersMetrics")) {
                String containerId = json.getJSONObject("header").getString("container-name");
                long unprocessedMessage = json.getJSONObject("metrics").
                        getJSONObject("org.apache.samza.system.SystemConsumersMetrics").
                        getLong("unprocessed-messages");
                unprocessedMessages.put(containerId, unprocessedMessage);
                //LOG.info("UnprocessedMessages information: "+ containerId +" ," + unprocessedMessage);
            }
        }
    }
    /*
     *
     * @param brokers brokers
     * @param groupId
     * @return
     */
    private static Properties createConsumerConfig(String brokers, String appName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, appName);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //props.put("auto.commit.enable", "false");

        return props;
    }
    public void start(){
        LOG.info("Start new Unprocessed Messages Monitor");
        if(t==null){
            t = new Thread(this::run, "Unprocessed Messages Monitor");
            t.start();
        }
    }
    public void stop(){
        LOG.info("Stop Unprocessed Messages Monitor");
        try{
            if(t!=null)t.join();
        }catch(Exception e){
            LOG.error(e.toString());
        }
    }
    /*
    public static void main(String[] args) throws InterruptedException {
        String brokers = args[0];
        // String groupId = args[1];
        String topic = args[1];
        String appName = args[2];
        Properties props = createConsumerConfig(brokers, appName);
        UnprocessedMessageMonitor example = new UnprocessedMessageMonitor(props, topic);
        example.appName = args[2];
        example.run();
        example.shutdown();
    }
    */
}