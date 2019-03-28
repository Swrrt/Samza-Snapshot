package org.apache.samza.zk;

import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import kafka.coordinator.MemberSummary;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.config.Config;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class MessageRateServer {
    private long totalArrival;
    private long totalService;
    private AdminClient adminClient;
    private List<String> topics;
    public MessageRateServer(Config config) {
        totalArrival = 0;
        totalService = 0;
        adminClient = AdminClient.create(getProps(config));
        topics = getTopics(config);
    }
    private List<String> getTopics(Config config){
        List<String> topics = new LinkedList<>();
        String string = config.get("kafka.topic");
        for(String topic:string.split(",")){
            topics.add(topic);
        }
        return topics;
    }
    public long[] getBacklog(String topic){
        // TODO: read partition offset from Kafka
       /*for(GroupOverview group: (List<GroupOverview>)adminClient.listAllConsumerGroups().values()){
           for(AdminClient.ConsumerSummary summary :(List<AdminClient.ConsumerSummary>)adminClient.describeConsumerGroup(group.groupId()).get()){
               ((List< TopicPartition>)summary.assignment()).get(0).
           }
       }*/
       return null;
    }
    private Properties getProps(Config config){
        Properties prop = new Properties();
        prop.putAll(config.subset("kafka"));
        return prop;
    }

    public void getInformation() {
    }
}
