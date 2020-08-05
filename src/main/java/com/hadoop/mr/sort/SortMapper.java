package com.hadoop.mr.sort;

import com.hadoop.mr.flowsum.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map阶段
 * KEYIN  输入数据的key
 * VALUEIN 输入数据的value
 * KEYOUT 输出数据的key
 * VALUEOUT 输出数据的value
 */
public class SortMapper extends Mapper<LongWritable, Text, SortBean,Text> {
    SortBean k = new SortBean();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 获取1行
        String s = value.toString();
        //2 切割
        String[] split = s.split("\t");
        //3 封装
        String phone = split[0];
        long upFlow = Long.parseLong(split[1]);
        long downFlow = Long.parseLong(split[2]);
        long sumFlow = Long.parseLong(split[3]);
        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);
        v.set(phone);
        //4 输出
        context.write(k,v);
    }
}
