package com;

/**
 * Created by kcz on 2017/2/18.
 */

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;


public class WordCountLocal implements Serializable {

    private static final long serialVersionUID = -5528440737714481080L;

    public static void main(String[] args) throws UnsupportedEncodingException {
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");


        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.setCheckpointDir("hdfs://localhost:9000/checkPoint");
        JavaRDD<String> lines = sc.textFile("D://Program Files//mongo//bin//tweets.bat");

//        Iterator<Tuple2<String, Integer>> timeIte = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                List<String> resultList = Arrays.asList(line.substring(0, line.indexOf(",")).split(","));
//                Iterator iterator = resultList.iterator();
//                return iterator;
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<>(s, 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v2, Integer v22) throws Exception {
//                return v2 + v22;
//            }
//        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                return new Tuple2(stringIntegerTuple2._2, stringIntegerTuple2._1);
//            }
//        }).sortByKey(false).mapToPair(s -> new Tuple2(s._2, s._1)).take(10).iterator();


        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String line) throws Exception {
                List<String> resultList = Arrays.asList(line.substring(line.lastIndexOf(",") + 1).split(","));
                Iterator iterator = resultList.iterator();
                return iterator;
            }

        });
//        while (timeIte.hasNext()) {
//            Tuple2<String, Integer> y = timeIte.next();
//            System.out.println(y._1 + "次数" + y._2);
//        }

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;


                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });
       // wordCounts.saveAsTextFile("hdfs://localhost:9000/scresult");
        //wordCounts.checkpoint();
        Iterator<Tuple2<String, Integer>> iterator = wordCounts
                //交换key-value，注意类型
                .mapToPair(s -> new Tuple2<Integer, String>(s._2, s._1))
                //倒序
                .sortByKey(false)
                //交换key-value，注意类型
                .mapToPair(s -> new Tuple2<String, Integer>(s._2, s._1)).take(10).iterator();


        while (iterator.hasNext()) {
            Tuple2<String, Integer> y = iterator.next();
            System.out.println(y._1 + "次数" + y._2);
        }

        sc.close();
    }

}