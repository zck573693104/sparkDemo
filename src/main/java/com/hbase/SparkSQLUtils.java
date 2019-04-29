package com.hbase;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
 
public class SparkSQLUtils {
 
    public static SparkSession getSparkSQL(List<String> tableNameList,List<String> columnList){
        //新建SparkSession
        SparkSession sparkSQL= getSparkSession();
        //new一个JavaSparkContext 对象
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSQL.sparkContext());
        //获取HBASE连接
        Configuration hbaseConfig = SparkHBaseUtils.getConfiguration();
 
        //循环遍历把要查询的表视图化
        for(String tableNameStr:tableNameList){
            hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableNameStr);
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = javaSparkContext.newAPIHadoopRDD(hbaseConfig, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
 
            //列RDD
            JavaRDD<List<String>> recordColumnRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, List<String>>() {
                public List<String>  call(Tuple2<ImmutableBytesWritable, Result> tuple) {
                    List<String> recordColumnList = new ArrayList();
                    Result result = tuple._2;
                    Cell[] cells = result.rawCells();
                    for (Cell cell : cells) {
                        recordColumnList.add(new String(CellUtil.cloneQualifier(cell)));
                    }
                    return recordColumnList;
                }
            });
 
            //数据RDD
            JavaRDD<Row> recordValuesRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, Row>() {
                public Row  call(Tuple2<ImmutableBytesWritable, Result> tuple) {
                    List<String> recordList = new ArrayList();
                    Result result = tuple._2;
                    Cell[] cells = result.rawCells();
                    for (Cell cell : cells) {
                        recordList.add(new String(CellUtil.cloneValue(cell)));
                    }
                    return (Row) RowFactory.create(recordList.toArray());
                }
            });
 
            //设置字段
            List<StructField> structFields=new ArrayList<StructField>();
//            if(columnList!=null){
//                for(String columnStr:columnList){
//                    structFields.add(DataTypes.createStructField(columnStr, DataTypes.StringType, true));
//                }
//            }else{
                List<String> fieldsList=recordColumnRDD.first();
                for(String columnStr:fieldsList){
                    structFields.add(DataTypes.createStructField(columnStr, DataTypes.StringType, true));
                }
//            }
 
            //新建列schema
            StructType schema=DataTypes.createStructType(structFields);
            Dataset employeesDataset= sparkSQL.createDataFrame(recordValuesRDD,schema);
            employeesDataset.printSchema();
            //spark表视图
            employeesDataset.createOrReplaceTempView(tableNameStr);
        }
        return sparkSQL;
    }
 
    public static Map getSQLConfig(String path,String fileName){
        Map tableConfigMap=null;
        List<String> tableList = new ArrayList<String>();
       //SQL文件路径
        String sqlPath=null;
        if(path==null){
            sqlPath="/user/app/hbase_table/sysconfig/sql/"+fileName+".sql";
        }else{
            sqlPath="/user/app/hbase_table/sysconfig/sql/"+path+"/"+fileName+".sql";
        }
        List<String> fileValueList = getFileValue(sqlPath);
        String sqlValueStr="";
        for(String lineStr:fileValueList){
                sqlValueStr=sqlValueStr+lineStr;
        }
//        try {
//            tableConfigMap = SQLAnalysisUtils.getSQLPares(sqlValueStr);
////            tableConfigMap.put("SQL",sqlValueStr);
//        } catch (JSQLParserException e) {
//            e.printStackTrace();
//        }
        return tableConfigMap;
    }
 
    /**
     * 输入文件路径[HDFS]读取文件内容
     * @param filePath
     * @return
     */
    private static List<String> getFileValue(String filePath){
        List<String> fileValueList = new ArrayList<>();
        Configuration conf = new Configuration();
        BufferedReader confBuff = null;
        try {
            //读取HDFS上的文件系统
            FileSystem fs = FileSystem.get(conf);
            // 调取任务的配置信息
            Path confPath = new Path(filePath);
            //设置流读入和写入
            InputStream confIn = fs.open(confPath);
            //使用缓冲流，进行按行读取的功能
            confBuff = new BufferedReader(new InputStreamReader(confIn));
            String confStr=null;
            String keyOneStr= null;
            String keyTwoStr=null;
            while((confStr=confBuff.readLine())!=null){
                //截取注释
                if(confStr.trim().length()!=0) {
                    if(confStr.trim().length()>=3){
                        keyOneStr = confStr.trim().substring(0, 3);
                        keyTwoStr = confStr.trim().substring(0, 2);
                        if (!keyOneStr.equals("/**") || !keyTwoStr.equals("--")) {
                            //本行无注释
                            if (confStr.indexOf("/**") == -1 && confStr.indexOf("--") == -1) {
                                fileValueList.add(confStr);
                            }
                            //本行以/**开头后面的注释包含--
                            if ((confStr.indexOf("/**") > -1 && confStr.indexOf("--") > -1) && (confStr.indexOf("/**") < confStr.indexOf("--"))) {
                                fileValueList.add(confStr.substring(0, confStr.indexOf("/**"))+" ");
                            }
                            //本行以--开头后面的注释包含/**
                            if ((confStr.indexOf("/**") > -1 && confStr.indexOf("--") > -1) && (confStr.indexOf("/**") > confStr.indexOf("--"))) {
                                fileValueList.add(confStr.substring(0, confStr.indexOf("--"))+" ");
                            }
                            //本行以/**注释开头
                            if (confStr.indexOf("/**") > -1 && confStr.indexOf("--") == -1) {
                                fileValueList.add(confStr.substring(0, confStr.indexOf("/**"))+" ");
                            }
                            //本行以--注释开头
                            if (confStr.indexOf("/**") == -1 && confStr.indexOf("--") > -1) {
                                fileValueList.add(confStr.substring(0, confStr.indexOf("--"))+" ");
                            }
                        }
                    }else{
                        fileValueList.add(confStr+" ");
                    }
                }
            }
            confBuff.close();
            confIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileValueList;
 
    }
 
 
    private static SparkSession getSparkSession(){
        SparkSession spark= SparkSession.builder()
                .appName("SparkApp")
                .master("local[2]")
                .getOrCreate();
        return spark;
    }
}