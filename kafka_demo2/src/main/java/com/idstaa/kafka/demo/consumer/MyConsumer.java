package com.idstaa.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author chenjie
 * @date 2021/3/10 22:30
 */
@Component
public class MyConsumer {
    @KafkaListener(topics = "topic-spring-01")
    public void onMessage(ConsumerRecord<Integer, String> record) {
        System.out.println("消费者收到消息"
                + record.topic() + "\t"
                + record.partition() + "\t"
                + record.offset() + "\t"
                + record.key() + "\t"
                + record.value());
    }
}
