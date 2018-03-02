package com;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class AvgScore {
    public static void main(String []args) {
        SparkConf sparkConf = new SparkConf().setAppName("top10").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("E:\\spark\\class");
        JavaPairRDD<String,Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split(" ")[0],Integer.valueOf(s.split(" ")[1]));
            }
        });
       // JavaPairRDD<String,Iterable<Integer>> iterableJavaPairRDD = pairRDD.groupByKey();
        JavaPairRDD<String,Integer> result = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        JavaPairRDD<String,Integer>pairRDD1 = pairRDD.reduceByKey((i1,i2)-> i1+i2);
        for (Tuple2<String,Integer> data1:pairRDD1.collect()){
            System.out.println(data1._1+":"+data1._2/3);
        }
        List<Tuple2<String,Integer>> list = pairRDD.collect();
        for (Tuple2<String,Integer> data:list){
           // System.out.println(data._1+":"+data._2);
        }

    }

}
