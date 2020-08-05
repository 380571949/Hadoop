package com.hadoop.mr.sort;

import com.hadoop.mr.flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN, VALUEIN   map阶段输出的k  v
 * KEYOUT, VALUEOUT  输出的k v
 */
public class SortReduce extends Reducer<SortBean, Text, Text, SortBean> {
    @Override
    protected void reduce(SortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text t : values){
            context.write(t,key);
        }
    }
}
