package com.idstaa.kafka.demo.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenjie
 * @date 2021/3/17 12:32
 */
public class MyProducer {
    public static void main(String[] args) {
        Map<String,Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        // 如果有多个拦截器，则设置为多个拦截器的全限定名，中间用逗号隔开
        configs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "com.idstaa.kafka.demo.interceptor.InterceptorOne,"
                +"com.idstaa.kafka.demo.interceptor.InterceptorTwo,"
                        +"com.idstaa.kafka.demo.interceptor.InterceptorThree");
        configs.put("classContent", "this is idstaa config");

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs);
        ProducerRecord<Integer,String > record = new ProducerRecord<>(
                "tp_inter_01", 0,
                1001,
                "this is lagou's 1001 message");


        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception==null){
                    System.out.println(metadata.topic());
                }
            }
        });

    }
}
