package com.chen.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Author:
 * Time:2020/7/14 20:13
 * 构建生产者
 */
public class ProductClient {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-1:9092,hadoop-2:9092,hadoop-3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++)
           // producer.send(new ProducerRecord<String, String>("bigdata20200715", Integer.toString(i), Integer.toString(i)));
            producer.send(new ProducerRecord<String, String>("bigdata20200715",2, Integer.toString(i), "itcast"+i));
        producer.close();
    }
}
