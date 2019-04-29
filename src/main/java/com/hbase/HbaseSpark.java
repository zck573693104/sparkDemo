package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class HbaseSpark {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Configuration hbaseConfig = HBaseConfiguration.create();
        //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost");
        //设置zookeeper连接端口，默认2181
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfig.set(TableInputFormat.INPUT_TABLE, "user");

        JavaPairRDD<ImmutableBytesWritable, Result> resultRDD = sc.newAPIHadoopRDD(hbaseConfig, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        long count = resultRDD.count();
        System.out.print("************SPARK from hbase  count ***************      " + count + "                 ");

        resultRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
            @Override
            public void call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                //获取行键
                String key = Bytes.toString(v1._2().getRow());
                //通过列族和列名获取列
                String name = Bytes.toString(v1._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
                System.out.println(name);
            }
        });

    }
}
