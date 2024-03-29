package com.xzz.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

/**
 * @author 徐正洲
 * @date 2022/3/10-18:16
 */
public class CustomConsumerSeek {
    public static void main(String[] args) {
//        配置
        Properties properties = new Properties();
//        连接kafka
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
//        key、value反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
//        创建消费组Id必须手动设置
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"ocean4");
//        修改分区分配策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.StickyAssignor");
//        创建消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
//        订阅主题
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);
//        指定offset位置开始消费
//        1) 通过assignment 获取分区信息
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
//        保证分区信息方案分配完毕
        while (assignment.size() == 0){
            kafkaConsumer.poll(Duration.ofSeconds(1));
//            分区值更新
            assignment = kafkaConsumer.assignment();
        }
//        2)指定位置
        for (TopicPartition topicPartition : assignment) {
            kafkaConsumer.seek(topicPartition,1572);
        }
//        拉取数据打印
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
//            打印消费到的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }


        }
        
    }

}