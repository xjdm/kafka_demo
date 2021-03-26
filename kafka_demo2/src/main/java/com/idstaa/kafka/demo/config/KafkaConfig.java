package com.idstaa.kafka.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;

/**
 * @author chenjie
 * @date 2021/3/10 22:39
 */
@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic topic1(){
        return  new NewTopic("nptc_01",3,(short)1);
    }

    @Bean
    public NewTopic topic2(){
        return  new NewTopic("nptc_02",5,(short)1);
    }

    @Bean
    public KafkaAdmin kafkaAdmin(){
        HashMap<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.server", "192.168.31.225");
        KafkaAdmin kafkaAdmin = new KafkaAdmin(configs);
        return kafkaAdmin;
    }

    @Bean
    @Autowired
    public KafkaTemplate<Integer,String> kafkaTemplate(ProducerFactory<Integer,String> producerFactory){
        HashMap<String, Object> configsOverride = new HashMap<>();
        configsOverride.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory,configsOverride);
        return template;
    }
}
