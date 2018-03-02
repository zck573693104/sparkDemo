package com;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class Friend {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("friend").setMaster("local");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = javaSparkContext.textFile("E:\\spark\\friend.txt");

        JavaPairRDD<List<String>, String> pairRDD = javaRDD.mapToPair(new PairFunction<String, List<String>, String>() {
            @Override
            public Tuple2<List<String>, String> call(String value) throws Exception {
                List<String> keyList = new ArrayList<>();
                StringTokenizer itr = new StringTokenizer(value);
                Set<String> set = new TreeSet<String>();
                String owner = itr.nextToken();
                while (itr.hasMoreTokens()) {
                    set.add(itr.nextToken());
                }
                String[] friends = new String[set.size()];
                friends = set.toArray(friends);
                for (int i = 0; i < friends.length; i++) {
                    for (int j = i + 1; j < friends.length; j++) {
                        String outputkey = friends[i] + friends[j];
                        keyList.add(outputkey);
                    }
                }
                return new Tuple2<>(keyList, owner);
            }
        });

        for (Tuple2<List<String>, String> tuple2 : pairRDD.collect()) {
            System.out.println(tuple2._1() + " " + tuple2._2());
        }
        Map<String, String> map = new HashMap<>();
        for (Tuple2<List<String>, String> tuple2 : pairRDD.collect()) {
            for (String key : tuple2._1) {
                if (map.containsKey(key)) {
                    map.put(key, map.get(key) + ":" + tuple2._2);
                } else {
                    map.put(key, tuple2._2);
                }
            }

        }
        for (Map.Entry entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        System.out.println("-------------------------------------");


    }
}
