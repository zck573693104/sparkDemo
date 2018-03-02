package com;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

public class Top10 {

    public static void main(String []args){
        SparkConf sparkConf = new SparkConf().setAppName("top10").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("E:\\spark\\top3.txt");

        JavaRDD<Integer> map = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return Integer.valueOf(s);
            }
        });

        JavaRDD<Integer> sort1 =   map.sortBy(new Function<Integer, Object>() {

            @Override
            public Object call(Integer integer) throws Exception {
                return integer;
            }
        },true,1);

        JavaRDD<Integer> sort = map.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer>4;
            }
        });

       List<Integer>sort2 =  sort1.collect();{
            for (Integer num : sort2) {
                System.out.println(num);
            }
        }
        List<Integer> nums = sort.top(3);
        for (Integer num : nums) {
            System.out.println(num);
        }
    }
}
