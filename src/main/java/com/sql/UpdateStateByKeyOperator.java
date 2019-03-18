package com.sql;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;


public class UpdateStateByKeyOperator {
    private static final String TOPIC = "test";
    public static Map<String, Object> kafkaParams = new HashMap<String, Object>();

    public static void main(String[] args) throws InterruptedException {
        kafkaParams.put("bootstrap.servers", "master:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList(TOPIC);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyDemo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
        jssc.sparkContext().setCheckpointDir("./checkPoint");

        JavaInputDStream<ConsumerRecord<String, String>> shopStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics,
                        kafkaParams));


        JavaDStream<String> words = shopStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> record) throws Exception {
                return Arrays.asList(record.value()).iterator();
            }
        });

        words.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return null;
            }
        });

        words.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                return null;
            }
        });
        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> stream = ones.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, org.apache.spark.api.java.Optional<Integer> state) throws Exception {
                /**
                 * values:经过分组最后 这个key所对应的value  [1,1,1,1,1]
                 * state:这个key在本次之前之前的状态
                 */
                Integer updateValue = 0;
                if (state.isPresent()) {
                    updateValue = state.get();
                }

                for (Integer value : values) {
                    updateValue += value;
                }
                return Optional.of(updateValue);
            }
        });
        stream.print();
        //开始流式计算
        jssc.start();
        // 等待计算终止
        jssc.awaitTermination();
        jssc.stop(true);
    }
}
