package com.idstaa.kafka.demo.controller;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

/**
 * @author chenjie
 * @date 2021/3/10 22:22
 */
@RestController
public class KafkaSyncProducerController {
    @Autowired
    private KafkaTemplate<Integer, String> template;

    @RequestMapping("/send/sync/{message}")
    public String send(@PathVariable String message) throws ExecutionException, InterruptedException {
        final ListenableFuture<SendResult<Integer, String>> future = template.send("topic-spring-01", 0, 0, message);
        final SendResult<Integer, String> sendResult = future.get();
        final RecordMetadata recordMetadata = sendResult.getRecordMetadata();
        System.out.println(recordMetadata.topic() + "\t" +
                recordMetadata.partition() + "\t" +
                recordMetadata.offset());
        return "success";
    }

}
