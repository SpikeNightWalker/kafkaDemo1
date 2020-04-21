package com.spike.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by Spike on 2020/4/19.
 * 自定义分区选择器，用于分配分区号
 */
public class MyPartitioner implements Partitioner{
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
//        Integer integer = cluster.partitionCountForTopic(topic);
        //返回分区号，这样消息就都发送到0号分区了
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
