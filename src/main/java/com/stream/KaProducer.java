package com.stream;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
public class KaProducer {
    public final static String TOPIC = "test";
    Producer<String, String> producer = null;
    private KaProducer() {
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("zk.connect", "127.0.0.1:2181");
        props.put("group.id", "1");
        // 配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        props.put("request.required.acks", "-1");

        props.put("partitioner.class", "com.stream.KafkaProducerPartitioner");

        Producer<String, String> producer = new KafkaProducer<>(props);
    }

    void produce() throws ExecutionException, InterruptedException {
        int messageNo = 1000;
        final int COUNT = 10000;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "WORD" + key;
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, data);
            RecordMetadata metadata = producer.send(record).get();
            String result =  record.value() + "] has been sent to partition " + metadata.partition();
            messageNo++;
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new KaProducer().produce();
    }
}
