package com.spike.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by Spike on 2020/4/19.
 * 绑定自定义拦截器的生产者
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "zk1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //加入拦截器
        ArrayList<String> list = new ArrayList<>();
        list.add("com.spike.kafka.interceptor.TimeInterceptor");
        list.add("com.spike.kafka.interceptor.CountInterceptor");
        //注意这里是put，老师的代码中，即便上面这些，也都是用的put，而非setProperties
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("fourth", "hello-" + i));
        }
        producer.close();
    }
}
