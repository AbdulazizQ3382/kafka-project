package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class Main {
    private static final Logger logger = LogManager.getLogger("log-app");
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-129zv.me-south-1.aws.confluent.cloud:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (Producer<String,String> producer = new KafkaProducer<String, String>(props)){
            String msg = "practice on kafka";
            producer.send(new ProducerRecord<>("goals",msg),(metadata,exception)->{
                if(exception !=null){
                    logger.error("error in kafka : ", exception);
                }
                else {
                    logger.debug("produced record %s at offset %s to topic %s ", msg, metadata.offset(), metadata.topic());
                }
            });
        }

    }
}