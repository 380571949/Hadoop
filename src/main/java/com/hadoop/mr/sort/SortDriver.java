package com.hadoop.mr.sort;

import com.hadoop.mr.flowsum.FlowBean;
import com.hadoop.mr.flowsum.FlowSumMapper;
import com.hadoop.mr.flowsum.FlowSumReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SortDriver {
    public static void main(String[] args) throws Exception {
        //1 获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 设置jar存储位置
        job.setJarByClass(SortDriver.class);
        //3 关联Map和Reduce类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);
        //4 设置Mapper阶段输出的kv类型
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(Text.class);
        //5 设置最终数据输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SortBean.class);
        //关联分区
        job.setPartitionerClass(SortPartitioner.class);
        job.setNumReduceTasks(5);
        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\sort\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\sortPart"));
        //7 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
