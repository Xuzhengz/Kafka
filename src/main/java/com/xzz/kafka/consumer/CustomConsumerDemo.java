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
public class CustomConsumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //TODO 设置kafka链接信息,多个节点用逗号分割
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        //TODO 反序列化 string即可不需要改
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //TODO 消费者组Id随机生成
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        //TODO 设置从topic起始数据开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        ArrayList<String> topics = new ArrayList<String>();
        //TODO 订阅接口返回的topic名称
        topics.add("");
        kafkaConsumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                //TODO 处理数据
                System.out.println(consumerRecord.value());
                if ("end".equals(consumerRecord.value())){
                    //TODO 消费完调接口删除临时topic和消费者组Id
                    break;
                }
            }
        }
    }
}