package com;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DistinctTest {
    public static void main(String []args){
        SparkConf sparkConf = new SparkConf().setAppName("top10").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("E:\\spark\\distinct.txt");
        JavaRDD<String> dis = lines.distinct();
        for (String s:dis.collect()){
            System.out.println(s);
        }
    }
}
