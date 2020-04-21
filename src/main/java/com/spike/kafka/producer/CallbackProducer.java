package com.spike.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by Spike on 2020/4/19.
 * 带回调函数的producer
 */
public class CallbackProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "zk1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            /*
                这个key会随着消息被发送出去，还有一个作用是，它会被hash，从而算出这条消息会被发送到那个分区，我们可以指定这个key，也可以不指定，但我们是看不到这个key的，以为消费端的控制台只打印value
                hash的时候，估计是：key.hashCode() % partition.size
                由于key是相同的，所以kafka算出第一个分区号后，就开始累加，下一条消息就发送到+1的分区号
                而我们此处明确指定了0号分区，我估计这个key就不会被hash了
            */
            producer.send(new ProducerRecord<>("fourth", 0, "spike", "hello-" + i), (recordMetadata, e) -> {
                //一般用下面这种方式
//            producer.send(new ProducerRecord<>("fourth", "hello-" + i), (recordMetadata, e) -> {
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
