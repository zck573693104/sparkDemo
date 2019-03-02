package com.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class DailyUvStreaming {

    public static void main(String[] args) throws InterruptedException {

        //设置日志级别
//        Logger.getLogger("org").setLevel(Level.ERROR);

        //初始化spark上下文
        SparkConf conf;
        conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("DailyUvStreaming");

        //初始化spark上下文 以及时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        //监听TCP服务
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        //将 DStream 转换成 DataFrame 并且运行sql查询
        lines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> rdd, Time time) {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

                //通过反射将RDD转换为DataFrame
                JavaRDD<UserAccessLog> rowRDD = rdd.map(new Function<String, UserAccessLog>() {
                    @Override
                    public UserAccessLog call(String line) {
                        UserAccessLog userLog = new UserAccessLog();
                        String[] cols = line.split(" ");
                        userLog.setDate(cols[0]);
                        userLog.setUserId(cols[1]);
                        return userLog;
                    }
                });
                Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, UserAccessLog.class);

                // 创建临时表
                dataFrame.createOrReplaceTempView("log");

                //按日期分组 去重userId,计算访客数
                Dataset<Row> result =
                        spark.sql("select date, count(distinct userId) as uv from log group by date");
                System.out.println("========= " + time + "=========");

                //输出前20条数据
                result.show();
            }
        });


        //开始流式计算
        jssc.start();
        // 等待计算终止
        jssc.awaitTermination();
        jssc.stop(true);


    }
}