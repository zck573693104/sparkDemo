package com;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Iterator;

public class AccumulatorTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumulatorTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator accumulator = sc.sc().longAccumulator("test_accu");
        JavaRDD<String> stringJavaRDD = sc.textFile("./data/top3.txt", 2);
        JavaRDD<String> mapRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                Broadcast<Integer> broadcast = sc.broadcast(5);
                return Integer.valueOf(s) > broadcast.getValue();
            }
        });
        mapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                accumulator.add(1);
                System.out.println(s);
            }
        });



    }
}
