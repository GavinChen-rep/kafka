package com.chen.kafka.com.chen.kafka.offset.AutoOffsetTest;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Author:
 * Time:2020/7/16 10:24
 */
public class Partition implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取分区个数
        Integer count = cluster.partitionCountForTopic(topic);
        Random random=new Random(count);
        int i = random.nextInt();
        return i;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
