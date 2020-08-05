package com.hadoop.mr.flowsum;

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
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text t=new Text();
    FlowBean bean=new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 获取1行
        String s = value.toString();
        //2 切割
        String[] split = s.split("\t");
        //3 封装
        t.set(split[1]);//手机号
        long upFlow=Long.parseLong(split[split.length-3]);
        long downFlow=Long.parseLong(split[split.length-2]);
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        //bean.set(upFlow,downFlow);
        //4 输出
        context.write(t,bean);
    }
}
