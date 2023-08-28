package com.xzz.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/3/6-20:18
 */
public class CustomProducerAck {
    public static void main(String[] args) {
        //        创建生产者配置信息
        Properties properties = new Properties();
//                创建生产者连接的broker
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
//        创建key，value全类名
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//                创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
//        定义ACK策略
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
//        定义重试次数默认为INT
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
//        创建send方法发送信息
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first","ocean"+i));
        }
//                关闭资源
        kafkaProducer.close();
    }
}