package com.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN, VALUEIN   map阶段输出的k  v
 * KEYOUT, VALUEOUT  输出的k v
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1 累加求和
        int sum=0;
        for(IntWritable v : values){
            sum+=v.get();
        }
        //2 写出
        intWritable.set(sum);
        context.write(key, intWritable);
    }
}
