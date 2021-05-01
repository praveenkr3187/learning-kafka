package com.praveen.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallback {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
        System.out.println("Hello Kafka");
        String bootStrapServer = "127.0.0.1:9092";
                
        //create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        
        for(int i =0; i < 10; i++){
            String topic = "first_topic";
            String text = "Hello world " + i;
            //create Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, text);
            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("\ntopic: " + recordMetadata.topic() +
                                "\npartition: " + recordMetadata.partition());
                    }else {
                        logger.error(e.getMessage());
                    }
                }
            }).get();

           
        }
        //flush and close
        producer.close();
    }
}
