package com.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;

import java.util.Properties;

public class HiveSpark {
    public static void main (String [] args){
    	System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession spark = SparkSession.builder().master("local[2]")
				.appName("SparkHive")
				.config("spark.sql.warehouse.dir", "/user/hive/warehouse/").enableHiveSupport()
				.getOrCreate();


        spark.sql("use zck_test");
        spark.sql("show tables").show();
        spark.sql("insert into test values (2,'is me')");
       Dataset<Row> dataset = spark.sql("select * from test");
       dataset.show();
       //数据库内容
		String url = "jdbc:mysql://localhost:5066/result?charSet=utf-8";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user","root");
		connectionProperties.put("password","123456");
		connectionProperties.put("driver","com.mysql.jdbc.Driver");

		//将数据通过覆盖的形式保存在数据表中
		dataset.write().mode(SaveMode.Overwrite).jdbc(url, "test", connectionProperties);



    }
}
