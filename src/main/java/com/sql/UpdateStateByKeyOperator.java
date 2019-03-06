package com.sql;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * UpdateStateByKey的主要功能:
 * 1、为Spark Streaming中每一个Key维护一份state状态，state类型可以是任意类型的， 可以是一个自定义的对象，那么更新函数也可以是自定义的。
 * 2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，Spark Streaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新
 * 
 * hello,3
 * spark,2
 * 
 * 如果要不断的更新每个key的state，就一定涉及到了状态的保存和容错，这个时候就需要开启checkpoint机制和功能 
 * 
 * 全面的广告点击分析
 * @author root
 *
 * 有何用？   统计广告点击流量，统计这一天的车流量，统计点击量
 */

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
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setCheckpointDir("hdfs://localhost:9000/checkPoint1");
         
        JavaInputDStream<ConsumerRecord<String, String>> shopStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics,
                            kafkaParams));



        JavaDStream<String> words  = shopStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> record) throws Exception {
                return Arrays.asList(record.key()).iterator();
            }
        });

        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>("1", 1);
            }
        });

          JavaPairDStream<String, Integer> stream =   ones.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, org.apache.spark.api.java.Optional<Integer> state) throws Exception {
                /**
                 * values:经过分组最后 这个key所对应的value  [1,1,1,1,1]
                 * state:这个key在本次之前之前的状态
                */
                Integer updateValue = 0 ;
                 if(state.isPresent()){
                     updateValue = state.get();
                 }

                 for (Integer value : values) {
                     updateValue += value;
                }
                return Optional.of(updateValue);
            }
        });
          stream.print();
        jssc.start();

        jssc.awaitTermination();

        jssc.close();
    }
}
