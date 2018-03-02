package com;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class StayTime implements Serializable {

    public static void main(String []args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("stayTime");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> lines = sparkContext.textFile("E:\\spark\\stay_time.txt");
        JavaPairRDD<String,String> map = lines.mapToPair(new PairFunction<String, String, String>() {

            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" +|\\t")[0],s.substring(11,s.length()).trim());
            }
        });

        List<String> list = new ArrayList<>();
        for (Tuple2<String,String>tuple2:map.collect()){
            list.add(tuple2._2);
            System.out.println(tuple2._1+" "+tuple2._2);
        }
        Collections.sort(list, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                String []arr1 = o1.split(" +|\\t");
                String []arr2 = o2.split(" +|\\t");
                return (arr1[3] + arr1[4]).compareTo((arr2[3] + arr2[4]));
            }
        });
        Collections.reverse(list);
        for (int i =0; i < list.size(); ++i) {
            System.out.println(list.get(i));
        }
    }
}
