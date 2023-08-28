package com.xzz.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author 徐正洲
 * @date 2022/3/6-21:01
 */
public class MyPartitioner implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //获取消息
        String message = o1.toString();

        //创建分区
        int partition;

        if (message.contains("ocean")){
            partition=1;
        }else {
            partition=2;
        }
        return partition;
    }
// 关闭资源
    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}