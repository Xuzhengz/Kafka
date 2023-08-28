package com.xzz.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/3/6-20:18
 */
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        //        创建生产者配置信息
        Properties properties = new Properties();
//                创建生产者连接的broker
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
//        创建key，value全类名
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,MyPartitioner.class.getName());
//                创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
//        创建send方法发送信息
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first","ocea" +i), new Callback() {
                // 该方法在 Producer 收到 ack 时调用，为异步调用
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e ==null){
                        System.out.println("主题：" + recordMetadata.topic() + "->" + "分区：" + recordMetadata.partition());
                    }else {
                        e.printStackTrace();
                        System.out.println("发送信息失败");
                    }

                }
            });
        }
//                关闭资源
        kafkaProducer.close();
    }
}