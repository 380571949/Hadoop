package com.hadoop.mr.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text,GroupBean, NullWritable> {
    GroupBean k=new GroupBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        k.setId(Integer.parseInt(split[0]));
        k.setPrice(Double.parseDouble(split[2]));
        context.write(k,NullWritable.get());
    }
}
