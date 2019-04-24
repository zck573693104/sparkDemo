package com.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class App 
{
    public static void main(String[] args) throws Exception {
        String warehouseLocation = "file:" + System.getProperty("user.dir") + "spark-warehouse";
        SparkSession spark = SparkSession
          .builder().master("local")
          .appName("Java Spark Hive Example")
          .config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport()
          .getOrCreate();



        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");



        //load from HDFS





         spark.sql("create table TEST.employee as select * from temp_table");



        }
}