package com.spike.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by Spike on 2020/4/19.
 * 通过main方法运行时，consumer会打印一堆debug信息
 * 我们加入logback.xml来屏蔽这些信息
 */
public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "zk1:9092");
        /*
            默认为true
            0.9版本kafka的offset是提交到zookeeper中的，0.10版本之后的kafka是提交到本地的bootstrap-server中（即本地磁盘中）
            开启自动提交offset，不开的话，就要手动提交，如果你最终没有提交offset的话，那永远会从某一个最后记录的offset开始消费
            不过开启后，这个offset不会实时往磁盘里写，而是每隔一段时间才往磁盘里写一次，期间在内存中会维护一个offset
            连续消费的时候，直接从内存中拿这个offset比较快，但同时也会往磁盘里写offset，只是不从磁盘中读，只有第一次启动consumer的时候，才会去磁盘里拿一次offset

            自动提交的缺点是，如果offset已经提交，但消费者还没处理完其中的数据，然后消费者挂掉了
            重启之后就会跳过刚才没处理完的数据
            这种情况，可能就需要手动提交offset，即：等我们处理完消息后再将offset写入磁盘
            而且自动提交的间隔时间不好设，1秒和10秒都未必合适
                1秒：消费者还没处理完消息就挂掉了，但offset已经提交了，导致那些消息没处理完
                10秒：消费者都处理完了，比如5秒就把消息都存入了mysql，然后还没到10秒的时候挂掉了，offset还没提交
                    重启后消费者又会重复读取这批消息，又往mysql里面存一次，导致数据重复

            生产环境中貌似一般用异步手动提交，自己管理offset，将offset用ConsumerRebalanceListener存入mysql
            mysql里面要加事务，需要group, topic, partition, offset等多列
            这样能保证消费者这边完全不丢失数据
         */
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //消费完后保存offset的时间间隔，只有开启了自动提交offset，这个时间才有用
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //反序列化
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //配置消费者组，这个组如果不存在的话，也是可以消费到数据的，但通过ZK看，底层貌似不会创建这个组
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "newGroup1");
        //重置消费者的offset，相当于命令中的--beginning，即从头开始消费，有两个前提，其中一个是换组，需要换新的消费者组才生效
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题，如果主题不存在，会有一个连接不上的警告，但不会报错
//        consumer.subscribe(Arrays.asList("fourth", "abc"));
        consumer.subscribe(Collections.singletonList("fourth"));


        while (true) {
            //消费数据，poll是轮询的意思
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "-" + consumerRecord.value());
            }
        }

    }
}
