package com.mlib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.File;
import java.util.ArrayList;


public class EnglishSpamJava {
   public static void main(String[] args) throws Exception{
       SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("EnglishSpam");
       JavaSparkContext jsc = new JavaSparkContext(conf);
       SparkSession session = SparkSession
               .builder()
               .config(conf)
               .getOrCreate();
       JavaRDD<String> lines = jsc.textFile("C:\\Users\\asus\\Desktop\\data\\spam_ham.txt");
       RDD<Row> rowRDD = lines.map(new Function<String, Row>() {
           @Override
           public Row call(String v1) throws Exception {
               String[] arr = v1.split("\t");
               double type = arr[0].equals("ham") ? 0.0 : 1.0;
               return RowFactory.create(type,arr[1]);
           }
       }).rdd();
       ArrayList<StructField> fields = new ArrayList<StructField>();
       fields.add(DataTypes.createStructField("label", DataTypes.DoubleType,true));
       fields.add(DataTypes.createStructField("content", DataTypes.StringType,true));
       StructType schema = DataTypes.createStructType(fields);
       Dataset<Row> data = session.createDataFrame(rowRDD, schema);
       // 分词器
       Tokenizer tkzer = new Tokenizer().setInputCol("content").setOutputCol("word");
       // 哈希桶词频分组
       HashingTF HTF = new HashingTF().setNumFeatures(1000).setInputCol("word").setOutputCol("features");
       // 逻辑回归算法
       LogisticRegression lRegress = new LogisticRegression().setMaxIter(20).setRegParam(0.1);
       // 管道器
       PipelineStage[] pp = new PipelineStage[3];
       pp[0] =  tkzer;
       pp[1] =  HTF;
       pp[2] =  lRegress;
       Pipeline pip = new Pipeline().setStages(pp);
       // 拟合数据，产生模型
       PipelineModel model = pip.fit(data);
       File file = new File("C:\\Users\\asus\\Desktop\\data\\email\\english\\model");
       if(file.list().length == 1){
           model.save("C:\\Users\\asus\\Desktop\\data\\email\\english\\model\\java");
       }

       // 模拟产生数据集进行测试
       ArrayList<Row> testRowList = new ArrayList<Row>();
       testRowList.add(RowFactory.create("I know you are. Can you pls open the back?"));
       testRowList.add(RowFactory.create("FreeMsg Why haven't you replied to my text? I'm Randy, sexy, female and live local. Luv to hear from u. Netcollex Ltd 08700621170150p per msg reply Stop to end"));
       testRowList.add(RowFactory.create("Hello, my love. What are you doing? Did you get to that interview today? Are you you happy? Are you being a good boy? Do you think of me?Are you missing me ?"));
       ArrayList<StructField> fields1 = new ArrayList<StructField>();
       fields1.add(DataTypes.createStructField("content",DataTypes.StringType,true));
       StructType schema1 = DataTypes.createStructType(fields1);
       Dataset<Row> testSet = session.createDataFrame(testRowList, schema1);
       Dataset<Row> predict = model.transform(testSet);
       predict.show();
       predict.createTempView("res");
       session.sql("select content,prediction from res").show();
   }
}
