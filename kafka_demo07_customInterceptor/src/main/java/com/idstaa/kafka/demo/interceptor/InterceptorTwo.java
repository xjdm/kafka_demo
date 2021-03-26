package com.idstaa.kafka.demo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author chenjie
 * @date 2021/3/17 12:44
 */
public class InterceptorTwo implements ProducerInterceptor<Integer,String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterceptorTwo.class);

    @Override
    public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
        System.out.println("拦截器2--go two");
        // 消息发送的时候，经过拦截器，调用该方法

        // 要发送的消息内容
        final String topic = record.topic();
        final Integer partition = record.partition();
        final Integer key = record.key();
        final String value = record.value();
        final Long timestamp = record.timestamp();
        final Headers headers = record.headers();

        // 拦截器拦下来之后根据原来的消息创建的新的消息
        ProducerRecord<Integer, String> newRecord = new ProducerRecord<>(
                topic,
                partition,
                timestamp,
                key,
                value,
                headers
        );

        return newRecord;

    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object classContent = configs.get("classContent");
        System.out.println(classContent);
    }
}
