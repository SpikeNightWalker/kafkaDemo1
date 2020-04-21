package com.spike.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Spike on 2020/4/19.
 * 使用自定义的分区选择器
 */
public class MyPartitionProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "zk1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //使用自定义的分区选择器，kafka显然会用反射来生成对象
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.spike.kafka.partitioner.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("fourth", "hello-" + i), (recordMetadata, e) -> {
                //如果是成功的
                if (null == e) {
                    System.out.println(recordMetadata.partition() + "--" + recordMetadata.offset());
                }else {
                    e.printStackTrace();
                }
            });
        }
        producer.close();
    }
}
