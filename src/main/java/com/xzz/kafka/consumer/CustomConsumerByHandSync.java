package com.xzz.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/3/10-18:16
 */
public class CustomConsumerByHandSync {
    public static void main(String[] args) {
//        配置
        Properties properties = new Properties();
//        连接kafka
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
//        key、value反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
//        创建消费组Id必须手动设置
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"ocean3");
//        设置为手动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

//        创建消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
//        订阅信息
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);
//        拉取数据打印
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
//            打印消费到的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

//            手动提交offset
            kafkaConsumer.commitAsync();


        }
        
    }

}