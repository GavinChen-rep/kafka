package com.chen.kafka.com.chen.kafka.offset.AutoOffsetTest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Author:
 * Time:2020/7/16 10:00
 */
public class Consumer {
    public static void main(String[] args) {
        //构建配置对象
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-1:9092,hadoop-2:9092,hadoop-3:9092");
        //指定消费者组的id
        props.put("group.id", "test03");
//        props.put("auto.offset.reset","earliest");
        //是否启用自动提交offset
        props.put("enable.auto.commit", "false");
        //提交的间隔时间
//        props.put("auto.commit.interval.ms", "1000");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //构建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
        //指定消费的Topic的
//        consumer.subscribe(Arrays.asList("bigdata20200714","bigdata20200715"));
        //指定消费某些分区的数据
        TopicPartition part0=new TopicPartition("bigdata20200714",1);
        TopicPartition part1=new TopicPartition("bigdata20200714",2);
        consumer.assign(Arrays.asList(part0,part1));
        //不断获取kafka中最新的数据
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition partition : partitions) {
                //从所有数据中获取当前这个分区的数据
                List<ConsumerRecord<String, String>> records1 = records.records(partition);
                //获取offset
                long offset=0;
                for (ConsumerRecord<String, String> record : records1) {
                    int part = record.partition();
                    String topic = record.topic();
                     offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    System.out.println("topic"+topic+"\t"+"offset:"+offset+"\t"+"partition"+part+"\t"+"key"+key+"\t"+"value"+value);
                }
                //手动提交这个分区的offset
                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(partition,new OffsetAndMetadata(offset+1));
                consumer.commitSync(offsets);
            }

        }
    }
}
