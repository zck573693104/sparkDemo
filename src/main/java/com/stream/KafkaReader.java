package com.stream;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class KafkaReader {
    static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // 每个话题的分片数
        int numThreads = 2;
        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                new Duration(50000));

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
                .createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics,
                                kafkaParams));

        JavaDStream<String> words = stream
                .flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
                    public Iterator<String> call(
                            ConsumerRecord<String, String> t) throws Exception {
                        System.out.println(">>>" + t.value());

                        return Lists.newArrayList(SPACE.split(t.value())).iterator();
                    }
                });

        // 对其中的单词进行统计
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        // 打印结果
        wordCounts.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
