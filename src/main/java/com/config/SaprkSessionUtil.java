package com.config;

import org.apache.spark.sql.SparkSession;

public class SaprkSessionUtil {

    private static final String METASTORE_URI = "thrift://master:9083";
    private static final String WAREHOUSE_DIR = "/user/hive/warehouse/";

    public static SparkSession getHiveSession(){

        SparkSession session = SparkSession.builder()
                .master("local")
                .config("hive.metastore.uris",METASTORE_URI)
                .config("dfs.client.use.datanode.hostname", "true")
                .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
                .enableHiveSupport()
                .getOrCreate();
        return session;
    }
}
