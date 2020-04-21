package com.spike.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by Spike on 2020/4/19.
 * 拦截器是加载生产者这端的
 * 本拦截器的作用是在所有message前面加个时间戳
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    /**
     * @param configs
     * 这个方法是先调用的，能获得配置信息
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = record.value();
        value = System.currentTimeMillis() + "-" + value;
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(record.topic(), record.partition(), record.key(), value);
        return producerRecord;
    }

    /**
     * 当获取到broker的ack时
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * 这种地方即便什么都不做，也不影响kafka最终关闭资源
     * 不会影响kafka调用它自身的close()方法
     * 上面的方法也一样
     */
    @Override
    public void close() {

    }


}
