package com.hadoop.mr.flowsum;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
@Data
public class FlowBean implements Writable {

    private long upFlow;//上行流量
    private long downFlow;//下行流量
    private long sumFlow;//总共流量

    //空参  方便反射使用
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        sumFlow=upFlow+downFlow;
    }

    /**
     * 序列化
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow=in.readLong();
        downFlow=in.readLong();
        sumFlow=in.readLong();
    }

    @Override
    public String toString() {
        return
                upFlow +
                "\t" + downFlow +
                "\t" + sumFlow
                ;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        sumFlow=upFlow+downFlow;
    }
}
