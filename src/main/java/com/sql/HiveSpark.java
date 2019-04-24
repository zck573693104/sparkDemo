package com.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class HiveSpark {
    public static void main (String [] args){
        SparkConf sparkConf = new SparkConf().setAppName("hive_spark").setMaster("spark://master:7077");
        SparkSession spark = SparkSession.builder().appName("hive_spark")
                .config(sparkConf).enableHiveSupport().getOrCreate();
       spark.sql("SELECT * FROM relation").show();

    }
}
