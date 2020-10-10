package com

import org.apache.spark.sql.functions.{col, lit, struct, udf}
import org.apache.spark.sql.{Row, SparkSession}

object SparkSqlTest {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    sparkSession.udf.register("strLen", strLen _)
    sparkSession.udf.register("allInOne", allInOne _)
    val ds = sparkSession.read.json("/load/data/test.json")


    ds.createTempView("test")
    val myConcatRowUDF = udf(myConcatRow)
    ds.select(myConcatRowUDF(struct(col("age"), col("name"), col("test")), lit("-"))).show
    sparkSession.sql(" select * from test").show()
    sparkSession.sql(" select strLen(name) from test").show()
    sparkSession.sql(" select allInOne(array(name,age,test),',') from test").show()
  }

  def allInOne(seq: Seq[Any], sep: String): String = seq.mkString(sep)


  def myConcatRow: ((Row, String) => String) = (row, sep) => row.toSeq.filter(_ != null).mkString(sep)

  def strLen(str: String): Integer = str.length
}
