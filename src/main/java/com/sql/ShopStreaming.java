package com.sql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ShopStreaming {

    public static void main(String[] args) throws InterruptedException {
            Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "master:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("test");
        //设置日志级别
//        Logger.getLogger("org").setLevel(Level.ERROR);

        //初始化spark上下文
        SparkConf conf;
        conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("ShopStreaming");

        //初始化spark上下文 以及时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaInputDStream<ConsumerRecord<String, String>> shopStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics,
                                kafkaParams));

        JavaDStream<ShopRating> shopRatingDStream = shopStream.map(new Function<ConsumerRecord<String, String>, ShopRating>() {
            @Override
            public ShopRating call(ConsumerRecord<String, String> record) throws Exception {
                ShopRating shopRating = JSON.parseObject(record.value(),ShopRating.class);
                return shopRating;
            }
        });

        JavaDStream<ShopRating> dStream = shopRatingDStream.window(Durations.seconds(40),Durations.seconds(20));
        dStream.print();
        System.out.println("次数:"+dStream.count());
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