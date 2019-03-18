package com.hadoop.reduce.reducer;

import com.hadoop.reduce.model.StaffModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 类或方法的功能描述 : 员工统计reduce类，统计员工的报销总数
 *
 * @author: logan.zou
 * @date: 2018-12-17 14:25
 */
public class StaffReduce extends Reducer<Text, StaffModel, Text, StaffModel> {

    /**
     * 读取 staffMap 的输出内容 内容格式  username StaffModel
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<StaffModel> values, Context context) throws IOException, InterruptedException {
        int money = 0;
        Text province = null;

        // 循环遍历员工数据
        for (StaffModel staffModel : values) {
            money += staffModel.getMoney().get();
            province = staffModel.getProvince();
        }

        // 输出汇总结果
        context.write(key, new StaffModel(key, new IntWritable(money), province));
    }
}
