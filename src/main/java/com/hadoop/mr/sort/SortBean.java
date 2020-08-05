package com.hadoop.mr.sort;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class SortBean implements WritableComparable<SortBean> {
    private long upFlow;//上行流量
    private long downFlow;//下行流量
    private long sumFlow;//总共流量

    //空参  方便反射使用
    public SortBean() {
        super();
    }

    public SortBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        sumFlow = upFlow + downFlow;
    }

    @Override
    public int compareTo(SortBean o) {
        //总流量倒序
        return this.sumFlow > o.getSumFlow() ? -1 : 1;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return
                upFlow +
                        "\t" + downFlow +
                        "\t" + sumFlow
                ;
    }
}
