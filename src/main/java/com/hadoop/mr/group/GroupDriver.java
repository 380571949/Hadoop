package com.hadoop.mr.group;

import com.hadoop.mr.sort.SortBean;
import com.hadoop.mr.sort.SortMapper;
import com.hadoop.mr.sort.SortPartitioner;
import com.hadoop.mr.sort.SortReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GroupDriver {
    public static void main(String[] args) throws Exception {
        //1 获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 设置jar存储位置
        job.setJarByClass(GroupDriver.class);
        //3 关联Map和Reduce类
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReduce.class);
        //4 设置Mapper阶段输出的kv类型
        job.setMapOutputKeyClass(GroupBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5 设置最终数据输出的kv类型
        job.setOutputKeyClass(GroupBean.class);
        job.setOutputValueClass(NullWritable.class);
        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\group\\GroupingComparator.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\group\\out"));
        job.setGroupingComparatorClass(GroupingComparator.class);
        //7 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
