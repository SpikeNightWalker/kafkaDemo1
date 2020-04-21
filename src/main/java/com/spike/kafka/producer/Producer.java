package com.spike.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Spike on 2020/4/19.
 */
public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();


        /*
            我们不需要记这些key
            我们只要记ProducerConfig和ConsumerConfig就行，这是两个配置参数常量类，里面还有相关说明
            这些说明也是以常量形式存放的，之所以没放在注释中，是为了报错时，能显示这些说明
         */
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "zk1:9092");
        //kafka 集群，等价于broker-list
        props.put("bootstrap.servers", "zk1:9092");

        /*
            以下配置中，有些可以不配，都有默认值
         */

        //ACK应答级别
        props.put("acks", "all");

        //重试次数
        props.put("retries", 1);

        //批次大小
        props.put("batch.size", 16384);

        //等待时间
        //批次大小和等待时间满足任意一个就会发送
        props.put("linger.ms", 1);

        //RecordAccumulator 缓冲区大小，批次的消息是先写入缓冲区后再发送的
        props.put("buffer.memory", 33554432);

        /*
            key/value的序列化类，此处我们的泛型是<String, String>，所以用StringSerializer
         */
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //这个泛型是数据的key/value类型
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            /*
                它还有一个重载的方法，接收一个callBack，回调时能获得zookeeper中的一些信息
                send方法默认是异步发送消息的
             */
            producer.send(new ProducerRecord<>("fourth", "spike-" + i));
        }

        /*
            如果数据既没有满足批次大小也没有满足等待时间的话，是不会被发送的
            除非调用close()方法
            当然，正常写代码，也一定要关闭资源，因为它里面还要做很多事情
            最好把close()方法写在try,catch,finally中，特别是在使用了while(true)时，在while外面加try
         */
        producer.close();





    }
}
