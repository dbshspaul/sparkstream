package com.sys.org;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * created by My System on 19-Jan-18
 **/
public class WriteIntoSparkStream {
    public static void main(String[] args) throws InterruptedException {
        org.apache.log4j.BasicConfigurator.configure();
        SparkConf conf = new SparkConf()
                .setAppName("spak-streaming")
                .setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("mytopic");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + record._2()));
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
