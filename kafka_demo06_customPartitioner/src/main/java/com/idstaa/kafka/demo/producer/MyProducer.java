package com.idstaa.kafka.demo.producer;

import com.idstaa.kafka.demo.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenjie
 * @date 2021/3/11 17:05
 */
public class MyProducer {
    public static void main(String[] args){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);

        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        // 此处不要设置partition的值
        ProducerRecord<String, String> record = new ProducerRecord<>("tp_part_01", "mykey", "myvalue");
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.out.println("消息发送失败");
                } else {
                    System.out.println(metadata.topic() + "\t"
                            + metadata.partition() + "\t"
                            + metadata.offset());
                }
            }
        });
        producer.close();
    }
}

