package com.sql;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import com.RedisUtil;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import java.util.*;

public class ShopStreaming {
    private static final String TOPIC = "test";
    private static final String GROUP = "test_group";
    public static Map<String, Object> kafkaParams = new HashMap<String, Object>();
    public static RedisUtil redisUtil = new RedisUtil();
    public static void main(String[] args) throws InterruptedException {
        kafkaParams.put("bootstrap.servers", "master:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList(TOPIC);

        SparkConf conf;
        conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("ShopStreaming");

        //初始化spark上下文 以及时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        Map<TopicPartition, Long> var2 = new HashMap<>();
        JavaInputDStream<ConsumerRecord<String, String>> shopStream = null;
        for (Map.Entry<String,String> entry:redisUtil.getAll(GROUP).entrySet()){
            TopicPartition topicPartition = new TopicPartition(entry.getKey(), Integer.valueOf(entry.getValue()));
            var2.put(topicPartition, redisUtil.getHash(entry.getKey(), entry.getValue()));
            shopStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics,
                            kafkaParams, var2));
        }
        if (var2.isEmpty()){
            shopStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics,
                            kafkaParams));
        }

        shopStream.transform(new Function2<JavaRDD<ConsumerRecord<String, String>>, Time, JavaRDD<Integer>>() {
            @Override
            public JavaRDD<Integer> call(JavaRDD<ConsumerRecord<String, String>> rdd, Time time) throws Exception {
                redisUtil = new RedisUtil();
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                for (OffsetRange offsetRange:offsets){
                    redisUtil.setHash(offsetRange.topic(),offsetRange.partition(),offsetRange.untilOffset());
                }
                JavaRDD<Integer> integerJavaRDD = rdd.map(new Function<ConsumerRecord<String, String>, Integer>() {

                    @Override
                    public Integer call(ConsumerRecord<String, String> record) throws Exception {
                        ShopRating shopRating = JSON.parseObject(record.value(), ShopRating.class);
                        return shopRating.getSkuId();
                    }
                });
                return integerJavaRDD;
            }
        }).print();
        JavaDStream<Integer> shopRatingDStream = shopStream.map(new Function<ConsumerRecord<String, String>, Integer>() {
            @Override
            public Integer call(ConsumerRecord<String, String> record) throws Exception {
                ShopRating shopRating = JSON.parseObject(record.value(), ShopRating.class);
                //System.out.println("测试"+shopRating.getSkuId());
                return shopRating.getSkuId();
            }
        });

        JavaDStream<Integer> dStream = shopRatingDStream.reduceByWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, Durations.seconds(40), Durations.seconds(20));



//        shopRatingDStream.foreachRDD(new VoidFunction2<JavaRDD<ShopRating>, Time>() {
//            @Override
//            public void call(JavaRDD<ShopRating> shopRatingJavaRDD, Time time) throws Exception {
//               SparkSession sparkSession = JavaSparkSessionSingleton.getInstance(shopRatingJavaRDD.context().getConf());
//               Dataset<Row> dataFrame = sparkSession.createDataFrame(shopRatingJavaRDD,ShopRating.class);
//               dataFrame.createOrReplaceTempView("shop");
//               sparkSession.sql("select * from shop").show();
//            }
//        });
        //开始流式计算
        jssc.start();
        // 等待计算终止
        jssc.awaitTermination();
        jssc.stop(true);


    }


}