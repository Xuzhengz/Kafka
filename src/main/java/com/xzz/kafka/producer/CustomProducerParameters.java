package com.xzz.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/3/6-20:18
 */
public class CustomProducerParameters {
    public static void main(String[] args) {
        //        创建生产者配置信息
        Properties properties = new Properties();
//                创建生产者连接的broker
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master1:9092,master2:9092");
//        创建key，value全类名
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //batch.size 16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        //linger.ms 默认0 修改为1
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        //RecordAccumlator 缓冲区大小默认32m
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        //compression.type 开启snappy压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
//                创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
//        创建send方法发送信息
        for (int i = 0; i < 50000; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first","ocean"+i));
        }
//                关闭资源
        kafkaProducer.close();
    }
}