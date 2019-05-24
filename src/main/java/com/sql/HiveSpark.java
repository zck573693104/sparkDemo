package com.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;

import java.util.Properties;

public class HiveSpark {
    public static void main(String[] args) {
        //System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession spark = SparkSession.builder()
                .master("local")
                .config("hive.metastore.uris", "thrift://winmaster:9083")
                .config("dfs.client.use.datanode.hostname", "true")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("show databases").show();

        spark.sql("use default");

        Dataset<Row> dataset = spark.sql("    SELECT\n" +
                "            dept. CODE AS bizCode,\n" +
                "            dept. NAME AS bizName,\n" +
                "            sale.soId AS soId,\n" +
                "        sum(Qty) AS saleQty,\n" +
                "        sum(TaxAmount) AS saleTaxAmount,\n" +
                "        sum(UnTaxAmount) AS saleUnTaxAmount,\n" +
                "        sum(TaxCostAmount) AS costTaxAmount,\n" +
                "        sum(UnTaxCostAmount) AS costUnTaxAmount\n" +
                "        FROM\n" +
                "        biz_tbl_sodcsale AS sale\n" +
                "        LEFT JOIN biz_tbl_department AS dept ON sale.SOId=dept.id\n" +
                "        LEFT JOIN biz_tbl_product AS product ON sale.productId=product.id\n" +
                "        LEFT JOIN biz_tbl_product_category AS category ON product.CategoryId=category.Id\n" +
                "        WHERE 1=1\n" +
                "          AND  dept.`Code` IS NOT NULL\n" +
                "        and sale.orderType=1\n" +
                "            and sale.dcId=-6895576767267386400\n" +
                "            and sale.bizDate>='2019-04-24'\n" +
                "            and sale.bizDate<='2019-05-23'\n" +
                "            and product.brandId =133814292812089381\n" +
                "            and sale.OrderCategoryId in(0,1,6484202360941265167,-6126895194400997038,6002033422858732428,-562550547340636210,5444773095477100579,8032642229039288004,7704128478807867969,-7041042811863413609,6342968152836230626,-2862621194036363213,-5701698999812667164,8828453056553894398,-7740066896991206631,-7561363930573222181,-2630891830353703715,873154323226316054,8712771461548822119,898989,4466054175909626931,3)\n" +
                "            AND (1=1)\n" +
                "            AND (1=1)\n" +
                "            group by sale.soId,bizCode,bizName\n" +
                "        HAVING bizCode is not null\n" +
                "        order by bizCode");
        dataset.show();
        //数据库内容
//		String url = "jdbc:mysql://localhost:5066/result?charSet=utf-8";
//		Properties connectionProperties = new Properties();
//		connectionProperties.put("user","root");
//		connectionProperties.put("password","123456");
//		connectionProperties.put("driver","com.mysql.jdbc.Driver");
//
//		//将数据通过覆盖的形式保存在数据表中
//		dataset.write().mode(SaveMode.Overwrite).jdbc(url, "test", connectionProperties);


    }
}
