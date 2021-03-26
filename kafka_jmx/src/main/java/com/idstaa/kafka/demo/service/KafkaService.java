package com.idstaa.kafka.demo.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

/**
 * @author chenjie
 * @date 2021/3/26 15:29
 */
@Service
public class KafkaService {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public String sendMessage(ProducerRecord<String, String> record)
            throws ExecutionException, InterruptedException {
        SendResult<String, String> result = this.kafkaTemplate.send(record).get();
        RecordMetadata metadata = result.getRecordMetadata();
        String returnResult = metadata.topic() + "\t" + metadata.partition() + "\t" +
                metadata.offset();
        System.out.println("发送消息成功：" + returnResult);
        return returnResult;
    }
}