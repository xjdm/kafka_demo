package com.idstaa.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author chenjie
 * @date 2021/3/10 17:36
 */
public class MyProducer1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        HashMap<String, Object> configs = new HashMap<>();
        // 指定初始化连接用到的broker地址
        configs.put("bootstrap.servers","192.168.31.225:9092");

        // 指定key 的序列化类
        configs.put("key.serializer", IntegerSerializer.class);

        // 指定value的序列化类
        configs.put("value.serializer", StringSerializer.class);

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(configs);

        List<Header> headers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {

            headers.add(new RecordHeader("biz.name", "producer.demo".getBytes()));
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(
                    "topic_1",
                    0,
                    i,
                    "hello idstaa 0",
                    headers
            );
            producer.send(record,new Callback(){
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        System.out.println("消息的主题"+metadata.topic());
                        System.out.println("消息的分区"+metadata.partition());
                        System.out.println("消息的偏移量"+metadata.offset());
                    }else {
                        System.out.println("异常消息："+exception.getMessage());
                    }
                }
            });
        }
        // 消息的同步确认
        /*final Future<RecordMetadata> future = producer.send(record);
        final RecordMetadata metadata = future.get();
        System.out.println("消息的主题"+metadata.topic());
        System.out.println("消息的分区"+metadata.partition());
        System.out.println("消息的偏移量"+metadata.offset());*/


        // 关闭生产者
        producer.close();

    }
}
