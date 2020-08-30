package com.chen.kafka.com.chen.kafka.offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Author:
 * Time:2020/7/14 20:30
 * 自定义消费者类
 */
public class ConsumerClientOffset {
    public static void main(String[] args) {
        //构建配置对象
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-1:9092,hadoop-2:9092,hadoop-3:9092");
        //指定消费者组的id
        props.put("group.id", "test03");
//        props.put("auto.offset.reset","earliest");
        //启用自动提交offset
        props.put("enable.auto.commit", "false");
        //提交的间隔时间
//        props.put("auto.commit.interval.ms", "1000");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //构建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
        //指定消费的Topic的
//        consumer.subscribe(Arrays.asList("bigdata2101","bigdata2102"));
        //指定消费某些分区的数据
        TopicPartition part0 = new TopicPartition("bigdata20200714", 0);
        TopicPartition part1 = new TopicPartition("bigdata20200714", 1);
        consumer.assign(Arrays.asList(part0,part1));
        //不断的获取Kafka中最新的数据
        while (true) {
            //定时到Kafka中获取最新的数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            //获取数据中所有的分区信息
            Set<TopicPartition> partitions = records.partitions();
            //取出每个分区
            for (TopicPartition partition : partitions) {
                //从所有数据中获取当前这个分区的数据
                List<ConsumerRecord<String, String>> partRecords = records.records(partition);
                //迭代取出这个 分区的每条数据
                long offset = 0;
                for (ConsumerRecord<String, String> record : partRecords){
                    //获取topic
                    String topic = record.topic();
                    //获取分区
                    int part = record.partition();
                    //获取offset
                    offset = record.offset();
                    //获取K和V
                    String key = record.key();
                    String value = record.value();
                    System.out.println(topic+"\t"+part+"\t"+offset+"\t"+key+"\t"+value);
                }

            }
        }
    }
}
