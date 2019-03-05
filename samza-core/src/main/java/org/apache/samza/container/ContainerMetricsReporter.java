package org.apache.samza.container;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
/*
    Reading AOL search data (AOL_search_data_leak_2006.zip) and send them to Kafka topic.
    Could start multiple generator on different host.
*/
public class ContainerMetricsReporter {
    private static final Logger LOG = LoggerFactory.getLogger(ContainerMetricsReporter.class);
    private String outputTopic;
    private String bootstrapServer;
    private final String containerId;
    private KafkaProducer producer;
    public ContainerMetricsReporter(String containerId){
        outputTopic = "ContainerMetrics";
        bootstrapServer = "yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096";
        this.containerId = containerId;
    }
    public ContainerMetricsReporter(String topic, String bootstrapServer, String containerId){
        outputTopic = topic;
        this.bootstrapServer = bootstrapServer;
        this.containerId = containerId;
    }

    public void init(Config config)throws InterruptedException{
        Properties props = setProps();
        //bootstrapServer = config.get("metrics.bootstrap.servers");
        producer = new KafkaProducer<String, String>(props);
        send(containerId + ": Container Start");
    }

    public void close(){
        send(containerId+": Container Stop");
        producer.close();
    }

    public Properties setProps(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrapServer);
        prop.put("client.id", containerId + "_ContainerMetricsReport");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return prop;
    }
    public void send(String line){
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, line);
        producer.send(record);
    }
}
