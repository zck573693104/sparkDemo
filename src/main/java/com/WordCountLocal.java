package com;

/**
 * Created by kcz on 2017/2/18.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;


public class WordCountLocal implements Serializable{

    private static final long serialVersionUID = -5528440737714481080L;

    public static void main(String[] args) {
        List<String> list = new ArrayList<String>();
        list.add("1");
        list.add("1");
        list.add("3");
        list.add("2");
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.parallelize(list);

        JavaRDD<String> words = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.trim();
            }
        });
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//
//            private static final long serialVersionUID = 1L;
//
//
//            public Iterable<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(" "));
//            }
//
//        });


        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;


                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });


        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;


            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }

        });

        sc.close();
    }

}