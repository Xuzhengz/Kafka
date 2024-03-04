package com.xzz.kafka.consumer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.util.IdUtil;
import lombok.extern.slf4j.Slf4j;
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
 * @date 2024/3/4 22:12
 */
@Slf4j
public class AirportConsumer {
    public static final String kafkaUrl = "10.28.21.131:30001,10.28.21.132:30001,10.28.21.133:30001";

    public static void main(String[] args) {
        String topic = args[0];
        String groupId = IdUtil.objectId();
        Properties properties = new Properties();
        //设置kafka链接信息,多个节点用逗号分割
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        System.out.println("consumeFusionData kafkaUrl : " + kafkaUrl);
        //反序列化 string即可不需要改
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //消费者组Id随机生成
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");     // 1000条
        //2、设置每批次大小
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1048576"); // 100m
        System.out.println("consumeFusionData groupId ： " + groupId);
        //设置从topic起始数据开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        ArrayList<String> topics = new ArrayList<String>();
        //订阅接口返回的topic名称
        System.out.println("topic :" + topic);
        topics.add(topic);
        kafkaConsumer.subscribe(topics);
        boolean flag = true;
        TimeInterval timer = DateUtil.timer();
        while (flag) {
            //加快拉取速度
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(300));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                if ("end".equals(consumerRecord.value())) {
                    flag = false;
                } else {
                    System.out.println(consumerRecord.value());
                }
            }
        }
        System.out.println("消费结束，共花费毫秒数：" + timer.interval());
        kafkaConsumer.close();

    }
}
