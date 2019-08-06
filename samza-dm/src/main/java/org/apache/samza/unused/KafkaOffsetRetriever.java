package org.apache.samza.unused;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.config.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
    Retrieve kafka offset information from console.

    !!! Currently not in use, since we cannot have multiple version of Kafka in one project
 */
public class KafkaOffsetRetriever {
    //private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetRetriever.class);
    private Properties properties;
    private String topic;
    private Map<Integer, Double> speed;
    private Map<Integer, Long> commited;
    long lastTime = 0;
    private double delta = 0.5; //Parameter to smooth processing speed
    public void initial(Config config, String topic_name){
        properties = new Properties();
        properties.putAll(config);
        topic = topic_name;
        speed = new HashMap<>();
        commited = new HashMap<>();
        lastTime = System.currentTimeMillis();
    }
    // Access Kafka server
    // Need kafka 2.2.0 server
    // Return a partitionId-backlog map
    public Map<Integer, Long> retrieveBacklog(){
        writeLog("Retrieving backlog from Kafka");
        AdminClient adminClient = AdminClient.create(properties);
        KafkaConsumer consumer = new KafkaConsumer(properties);
        Map<TopicPartition, OffsetAndMetadata> commitedOffset = new HashMap<>();
        Map<TopicPartition, Long> endOffset;
        Map<Integer, Long> backlog = new HashMap<>();
        for(int groupId = 0; groupId < 100; groupId++) {  //modify groupId range in KafkaSystemFactory.getConsumer()
            try {
                commitedOffset.putAll(adminClient.listConsumerGroupOffsets(String.valueOf(groupId)).partitionsToOffsetAndMetadata().get());
            } catch (Exception e) {
                writeLog("Exception when retrieve offsets from Kafka: " + e);
            }
        }
        endOffset = consumer.endOffsets(commitedOffset.keySet());
        for(TopicPartition topicPartition: endOffset.keySet()){
            if(topicPartition.topic().equals(topic)){
                backlog.put(topicPartition.partition(),endOffset.get(topicPartition) - commitedOffset.get(topicPartition).offset());
            }
        }
        writeLog("Retrieved backlog information: " + backlog.toString());
        return backlog;
    }
    // Access Kafka server
    // Return a containerId-processSpeed map
    public Map<Integer, Double> retrieveSpeed(){
        writeLog("Retrieving speed information from Kafka");
        AdminClient adminClient = AdminClient.create(properties);
        KafkaConsumer consumer = new KafkaConsumer(properties);
        Map<TopicPartition, OffsetAndMetadata> commitedOffset = null;
        Map<TopicPartition, Long> endOffset;
        for(int groupId = 0; groupId < 100; groupId++) {
            try {
                commitedOffset = adminClient.listConsumerGroupOffsets(String.valueOf(groupId)).partitionsToOffsetAndMetadata().get();
            } catch (Exception e) {
                writeLog("Exception when retrieve offsets from Kafka: " + e);
            }
        }
        endOffset = consumer.endOffsets(commitedOffset.keySet());
        long time = System.currentTimeMillis();
        for(TopicPartition topicPartition: endOffset.keySet()){
            if(topicPartition.topic().equals(topic)){
                long lastCommited = 0;
                if(commited.containsKey(topicPartition.partition())){
                    lastCommited = commited.get(topicPartition.partition());
                }
                commited.put(topicPartition.partition(), endOffset.get(topicPartition) - lastCommited);
                double lastSpeed = 0;
                if(speed.containsKey(topicPartition.partition())){
                    lastSpeed = speed.get(topicPartition.partition());
                }
                double newSpeed = delta * lastSpeed;
                newSpeed += (1 - delta) * (endOffset.get(topicPartition) - lastCommited)/((double)time - lastTime);
                speed.put(topicPartition.partition(), newSpeed);
            }
        }
        lastTime = time;
        writeLog("Retrieved speed information: " + speed.toString());
        return speed;
    }
    private void writeLog(String log){
        System.out.println("KafkaOffsetRetriever: " + log);
    }
}
