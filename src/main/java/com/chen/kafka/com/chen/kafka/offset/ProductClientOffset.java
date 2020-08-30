package com.chen.kafka.com.chen.kafka.offset;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Author:
 * Time:2020/7/14 20:13
 * 构建生产者
 */
public class ProductClientOffset {
    public static void main(String[] args) {
        //构建属性配置对象
        Properties props = new Properties();
        //指定Kafka的地址
        props.put("bootstrap.servers", "hadoop-1:9092,hadoop-2:9092,hadoop-3:9092");
        /**
         * 指定生产者生产数据时使用同步还是异步的方式
         *  0-异步，生产者不断的发送数据给Kafka，不管Kafka是否存储成功，不需要确认
         *  1-同步：生产者发送数据写入Kafka，如果Kafka对应分区的主副本写入成功，就返回ack，生产者发送下一条
         *  all-同步：生产者发送数据写入Kafka，如果Kafka对应分区的所有副本写入成功，就返回ack，生产者发送下一条
         */
        props.put("acks", "all");
        //重试次数
        props.put("retries", 0);
        //批量发送数据到Kafka
        props.put("batch.size", 16384);
        //间隔时间
        props.put("linger.ms", 1);
        //数据缓存的大小
        props.put("buffer.memory", 33554432);
        //key和Value序列化的类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //构建生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            /**
             * 分区方式
             *  第一种：如果你生产的数据给定了Key，按照Key的hash取余分区的个数
             *  第二种：如果不给定Key，按照轮询
             *  第三种：手动指定将数据写入哪个分区
             *  第四种：自定义分区
             */
            //生产数据到Kafka中
            producer.send(new ProducerRecord<String, String>("bigdata20200714", Integer.toString(i), "itcast" + i));
            //不带Key
//            producer.send(new ProducerRecord<String,String>("bigdata2101","itcast"+i));
            //指定分区
//            producer.send(new ProducerRecord<String, String>("bigdata2101", 1,Integer.toString(i), "itcast"+i));
        }
        //释放
        producer.close();
    }
}
