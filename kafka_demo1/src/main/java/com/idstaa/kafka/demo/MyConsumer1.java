package com.idstaa.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Consumer;

/**
 * @author chenjie
 * @date 2021/3/10 21:45
 */
public class MyConsumer1 {
    public static void main(String[] args) {
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.225:9092");
        // 使用常量代替手写的字符串，配置key的反序列化器
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        // 配置value的反序列化器
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 配置消费组ID
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_demo2");
        // 如果找不到当前消费的有效偏移量，则自动重置到最开始
        // lastest 表示重置到消息偏移量得到最后一个
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(configs);

        // 先订阅，再消费
        consumer.subscribe(Arrays.asList("topic_1"));
        while (true){
            final ConsumerRecords<Integer, String> consumerRecords = consumer.poll(3_000);
            // 遍历本次从主题分区拉取的消息
            consumerRecords.forEach(new Consumer<ConsumerRecord<Integer, String>>() {
                @Override
                public void accept(ConsumerRecord<Integer, String> record) {
                    System.out.println(record.topic() + "\t" +
                            record.partition() + "\t" +
                            record.offset() + "\t" +
                            record.key() + "\t" +
                            record.value());
                }
            });
        }
        // 如果主题中没有可以消费的消息，则该方法可以放到while循环中，每过3s重新拉取一次
        // 如果还没有拉取到，过3s再拉取，防止while循环太密集的poll调用
        // final ConsumerRecords<Integer, String> consumerRecords = consumer.poll(3_000);


       // consumer.close();


    }
}
