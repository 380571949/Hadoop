package com.hadoop.mr.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    String name;
    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件并做标记
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name=inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        //按文件处理
        if(name.startsWith("order")){
//            1001	01	1
            String[] split = s.split("\t");
            v.setOrderId(split[0]);
            v.setPId(split[1]);
            v.setAmount(Integer.parseInt(split[2]));
            v.setPname("");
            v.setFlag("order");
            k.set(split[1]);
        }else{
//            01	小米
            String[] split = s.split("\t");
            v.setOrderId("");
            v.setPId(split[0]);
            v.setAmount(0);
            v.setPname(split[1]);
            v.setFlag("pd");
            k.set(split[0]);
        }
        context.write(k,v);
    }
}
