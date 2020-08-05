package com.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
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
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text k = new Text();
    private IntWritable v=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        atguigu atguigu
//        ss ss
        // 1 获取1行
        String s = value.toString();
        // 2 切分
        String[] words = s.split(" ");
        // 3 循环写出
        for (String str : words) {
            k.set(str);
            context.write(k, v);
        }
    }
}
