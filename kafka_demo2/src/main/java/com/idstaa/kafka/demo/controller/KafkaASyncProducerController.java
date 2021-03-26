package com.idstaa.kafka.demo.controller;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

/**
 * @author chenjie
 * @date 2021/3/10 22:22
 */
@RestController
public class KafkaASyncProducerController {
    @Autowired
    private KafkaTemplate<Integer, String> template;

    @RequestMapping("/send/async/{message}")
    public String send(@PathVariable String message) throws ExecutionException, InterruptedException {
        final ListenableFuture<SendResult<Integer, String>> future = template.send("topic-spring-01", 0, 1, message);
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("发送消息失败：" + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                RecordMetadata metadata = result.getRecordMetadata();
                System.out.println("发送消成功：" + metadata.topic() + "\t" + metadata.partition() + "\t" + metadata.offset());

            }
        });

        return "success";
    }

}
