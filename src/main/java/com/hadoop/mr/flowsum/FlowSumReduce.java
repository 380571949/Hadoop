package com.hadoop.mr.flowsum;

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
public class FlowSumReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean bean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1 累加求和
        long sumUpFlow=0;
        long sumDownFlow=0;
        for(FlowBean flowBean : values){
            sumUpFlow+=flowBean.getUpFlow();
            sumDownFlow+=flowBean.getDownFlow();
        }
        bean.set(sumUpFlow, sumDownFlow);
        //2 输出
        context.write(key,bean);
    }
}
