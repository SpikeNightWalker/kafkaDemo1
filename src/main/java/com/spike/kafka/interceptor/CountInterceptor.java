package com.spike.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Spike on 2020/4/19.
 * 计数拦截器
 * 统计成功发送了几次，失败了几次
 *
 * 这个拦截器其实可以跟TimeInterceptor合成一个拦截器
 * 但此处为了演示多拦截器的情况，因此这里定义了一个新的拦截器
 */
public class CountInterceptor implements ProducerInterceptor{

    private AtomicInteger success = new AtomicInteger(0);
    private AtomicInteger error = new AtomicInteger(0);

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    /**
     * 当获取到broker的ack时
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (null != metadata) {
            success.incrementAndGet();
        }else{
            error.incrementAndGet();
        }
    }

    @Override
    public void close() {
        System.out.println("success: " + success);
        System.out.println("error: " + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
