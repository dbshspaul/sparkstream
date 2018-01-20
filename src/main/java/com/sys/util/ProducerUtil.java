package com.sys.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerUtil {
    private ProducerUtil() {
    }

    private static ProducerUtil getInstance() {
        org.apache.log4j.BasicConfigurator.configure();
        return new ProducerUtil();
    }

    private Producer getProducer() {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer(configProperties);
    }

    public static void sendMessage(String topicName, String message) {
        Producer producer = getInstance().getProducer();
        try {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, message);
            producer.send(rec);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}