package org.apache.samza.job.dm;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class DMTestKafkaInputGenerator {
    public static void main(String[] args){
        String inputStream = "WordCount";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        int throughput = 100;

        int count = 0;
        while (true) {
            String data = "random data";
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("WordCount", data);
            producer.send(record);

            count++;
            if (count > 3000) throughput = 250;

            try {
                Thread.sleep(1000/throughput );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
