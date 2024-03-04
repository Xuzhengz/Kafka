package com.xzz.kafka.consumer;


import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
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
public class CustomConsumer {
    public static void main(String[] args) {
//        配置
        Properties properties = new Properties();
//        连接kafka
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.11.151:9092,172.16.11.152:9092,172.16.11.153:9092");
//        key、value反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        创建消费组Id必须手动设置
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ocean20240301");
//        修改分区分配策略
//        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.StickyAssignor");
//        创建消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
//        订阅信息
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("radarTempf81cf37fb4764aaa91f5d5bf0c1440e6");
        kafkaConsumer.subscribe(topics);
//        拉取数据打印
        boolean flag = true;
        TimeInterval timer = DateUtil.timer();
        while (flag) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
//            打印消费到的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
                if ("end".equals(consumerRecord.value())) {
                    flag = false;
                }
            }
        }
        timer.interval();//花费毫秒数
        //关闭消费者客户端
        kafkaConsumer.close();
    }

}