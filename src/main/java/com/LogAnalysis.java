package com;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Random;

public class LogAnalysis {
    public static void main(String []args){
        SparkConf conf = new SparkConf().setAppName("logAnalysis").setMaster("local");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile("E:\\spark\\log.txt");

        JavaRDD<String> filters = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.length()>0;
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("GET")||s.contains("POST");
            }
        });
        JavaPairRDD<String,Integer> pairRDD = filters.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                if (s.contains("GET")){

                    return new Tuple2<String, Integer>(s.substring(s.indexOf("GET"),s.indexOf("HTTP")).trim(),1);
                }
                else {
                    return new Tuple2<String, Integer>(s.substring(s.indexOf("POST"),s.indexOf("HTTP")).trim(),1);
                }
            }
        });

        JavaPairRDD<String,Integer> reduce = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });


        for (Tuple2<String,Integer> tuple2:pairRDD.collect()){
            System.out.println(tuple2._1);
            System.out.println(tuple2._2);
            Random r = new Random();

        }
        for (Tuple2<String,Integer> tuple2:reduce.collect()){
            System.out.println(tuple2._1+":"+tuple2._2);
        }
    }

}
