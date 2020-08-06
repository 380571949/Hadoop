package com.hadoop.mr.group;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class GroupBean implements WritableComparable<GroupBean> {
    private int id;
    private double price;

    public GroupBean() {
        super();
    }

    public GroupBean(int id, double price) {
        super();
        this.id = id;
        this.price = price;
    }

    @Override
    public int compareTo(GroupBean o) {
        //先id升排序 相同按price降序排序
        int result;
        if (id > o.getId()) {
            result = 1;
        } else if (id < o.getId()) {
            result = -1;
        } else {
            result= Double.compare(o.getPrice(), price);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        price = in.readDouble();
    }

    @Override
    public String toString() {
        return
                id +
                        "\t" + price;
    }
}
