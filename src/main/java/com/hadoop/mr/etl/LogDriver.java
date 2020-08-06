package com.hadoop.mr.etl;

import com.hadoop.mr.group.GroupBean;
import com.hadoop.mr.group.GroupMapper;
import com.hadoop.mr.group.GroupReduce;
import com.hadoop.mr.group.GroupingComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LogDriver {
    public static void main(String[] args) throws Exception {
        //1 获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 设置jar存储位置
        job.setJarByClass(LogDriver.class);
        //3 关联Map和Reduce类
        job.setMapperClass(LogMapper.class);
        //5 设置最终数据输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\etl\\web.log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\etl\\out"));
        job.setGroupingComparatorClass(GroupingComparator.class);
        //7 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
