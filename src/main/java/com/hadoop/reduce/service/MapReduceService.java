package com.hadoop.reduce.service;

import com.hadoop.util.HdfsUtil;
import com.hadoop.util.ReduceJobsUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.stereotype.Service;


/**
 * 类或方法的功能描述 : 单词统计
 *
 * @author: logan.zou
 * @date: 2018-12-05 19:02
 */
@Service
public class MapReduceService {
    // 默认reduce输出目录
    private static final String OUTPUT_PATH = "/output";

    /**
     * 单词统计，统计某个单词出现的次数
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void wordCount(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job,如果输出路径存在则删除，保证每次都是最新的
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.getWordCountJobsConf(jobName, inputPath, outputPath);
    }

    /**
     * 单词统计,统计所有分词出现的次数
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void newWordCount(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job,如果输出路径存在则删除，保证每次都是最新的
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.wordCount(jobName, inputPath, outputPath);
    }

    /**
     * 一年最高气温统计
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void weather(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        JobConf jobConf = ReduceJobsUtils.getWeatherJobsConf(jobName);
        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        JobClient.runJob(jobConf);
    }

    /**
     * 员工统计,对象序列化
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void staff(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.staff(jobName, inputPath, outputPath);
    }

    /**
     * 员工统计，带排序的对象序列化
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void sort(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.sort(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 表join操作
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void join(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.join(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 获取共同好友
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void friends1(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.friends1(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 计算共同好友
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void friends2(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.friends2(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 分组统计
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void groupOrder(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.groupOrder(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 带计数器的单词统计
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void counter(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.counter(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 明星微博统计
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void weibo(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.weibo(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 明星微博搜索指数分析
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void weibo2(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.weibo2(jobName, inputPath, outputPath);
    }

    /**
     * mapreduce 分组统计、排序
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void groupSort(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsUtil.existFile(outputPath)) {
            HdfsUtil.deleteFile(outputPath);
        }
        ReduceJobsUtils.groupSort(jobName, inputPath, outputPath);
    }
}

