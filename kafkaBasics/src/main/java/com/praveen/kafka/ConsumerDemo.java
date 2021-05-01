package com.praveen.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        System.out.println("Hello Consumer");

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootStrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my_second_app";
        
        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        
        //subscripe to topic
        consumer.subscribe(Arrays.asList(topic));
        
        //poll
        while (true) {
            ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record: consumerRecord){
                logger.info("\nKey = " + record.key() + "\tvalue = " + record.value());
            }
        }
    }
}
