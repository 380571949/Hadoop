package com.hadoop.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 获取一行
        String s = value.toString();
        //2 解析
        boolean result = parseLog(s, context);
        if (!result) {
            return;
        }

        //3 输出
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String s, Context context) {
        String[] split = s.split(" ");
        if(split.length>11){
            context.getCounter("map","true").increment(1);
            return true;
        }else{
            context.getCounter("map","false").increment(1);
            return false;
        }

    }
}
