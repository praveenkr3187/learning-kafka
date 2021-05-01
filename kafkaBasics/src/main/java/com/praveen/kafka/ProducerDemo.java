package com.praveen.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello Kafka");
        String bootStrapServer = "127.0.0.1:29092";
                
        //create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        
        //create Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Ball");
        //send data
        producer.send(producerRecord);
        
        //flush and close
        producer.close();
    }
}
