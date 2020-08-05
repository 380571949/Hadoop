package com.hadoop.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartitioner extends Partitioner<SortBean, Text> {
    //按照手机号前三位分区
    @Override
    public int getPartition(SortBean key, Text value, int i) {
        String s = value.toString().substring(0, 3);
        int p=4;
        if("136".equals(s)){
            p=0;
        }else if("137".equals(s)){
            p=1;
        }else if("138".equals(s)){
            p=2;
        }else if("139".equals(s)){
            p=3;
        }
        return p;
    }
}
