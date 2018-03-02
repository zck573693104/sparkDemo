package com;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;


public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\spark\\top3.txt");

        //经过map映射，形成键值对的形式。
        JavaPairRDD<Integer, Integer> mapToPairRDD = lines.mapToPair(new PairFunction<String, Integer, Integer>() {


            private static final long serialVersionUID = 1L;

            int i = 0;

            public Tuple2<Integer, Integer> call(String num) throws Exception {

                int numObj = Integer.parseInt(num);
                Tuple2<Integer, Integer> tuple2 = new Tuple2<Integer, Integer>(numObj, i);
                return tuple2;
            }
        });
        /**
         * 1、通过sortByKey()算子，根据key进行降序排列
         * 2、排序完成后，通过map()算子获取排序之后的数字
         */
        JavaRDD<Integer> resultRDD = mapToPairRDD.sortByKey(false).map(new Function<Tuple2<Integer, Integer>, Integer>() {
            private static final long serialVersionUID = 1L;


            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._1;
            }
        });
        //通过take()算子获取排序后的前3个数字
        List<Integer> nums = resultRDD.take(3);
        for (Integer num : nums) {
            System.out.println(num);
        }
        sc.close();
    }
}