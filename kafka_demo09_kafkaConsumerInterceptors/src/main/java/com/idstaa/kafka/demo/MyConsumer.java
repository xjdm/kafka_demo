package com.idstaa.kafka.demo;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author chenjie
 * @date 2021/3/21 18:55
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "mygrp");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "myclient");
        // 如果在kafka中找不到当前消费者的偏移量，则设置为最旧的
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.idstaa.kafka.demo.interceptor.OneInterceptor"+
                ",com.idstaa.kafka.demo.interceptor.TwoInterceptor"+
                ",com.idstaa.kafka.demo.interceptor.ThreeInterceptor");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
        consumer.subscribe(Collections.singleton("tp_demo_01"));
        while(true){
            // 如果服务器没有数据，onCommit 拦截器会调用
            final ConsumerRecords<String, String> records = consumer.poll(3_000);
            records.forEach(record->{
                System.out.println(record.topic()+"\t"
                +record.partition()+"\t"
                +record.key()+"\t"
                +record.value());
            });
        }


    }
}
