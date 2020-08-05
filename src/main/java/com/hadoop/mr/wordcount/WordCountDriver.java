package com.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        //1 获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);
        //3 关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //4 设置Mapper阶段输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5 设置最终数据输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\hello.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\helloOut"));
        //7 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
