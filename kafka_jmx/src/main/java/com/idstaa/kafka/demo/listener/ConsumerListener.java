package com.idstaa.kafka.demo.listener;

import com.idstaa.kafka.demo.service.KafkaRetryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author chenjie
 * @date 2021/3/26 15:29
 */
@Component
public class ConsumerListener {
    private static final Logger log = LoggerFactory.getLogger(ConsumerListener.class);
    @Autowired
    private KafkaRetryService kafkaRetryService;
    private static int index = 0;

    @KafkaListener(topics = "${spring.kafka.topics.test}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        try {// 业务处理
            log.info("消费的消息：" + record);
            index++;
            if (index % 2 == 0) {
                throw new Exception("该重发了");
            }
        } catch (Exception e) {
            log.error(e.getMessage()); // 消息重试
            kafkaRetryService.consumerLater(record);
        }
    }
}
