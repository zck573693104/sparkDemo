package com.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import com.alibaba.fastjson.JSONObject;
import com.sql.ShopRating;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KaProducer {
    public final static String TOPIC = "test";
    Producer<String, String> producer = null;
    Random rand = new Random();
    int i = 1;
    private KaProducer() {
        // 此处配置的是kafka的端口
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "master:9092");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("group.id", "test_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        producer = new KafkaProducer<>(kafkaParams);
    }

    public void produce() throws ExecutionException, InterruptedException {
        while (true) {
            ShopRating shopRating = new ShopRating(i,i++);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, JSONObject.toJSONString(shopRating));
            RecordMetadata metadata = producer.send(record).get();
            String result =  record.value() + "] has been sent to partition " + metadata.partition();
            System.out.println(result);
            Thread.sleep(2000);
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new KaProducer().produce();
    }
}
