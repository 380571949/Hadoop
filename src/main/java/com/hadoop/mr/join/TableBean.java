package com.hadoop.mr.join;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
@Data
public class TableBean implements Writable {
    private String orderId; // 订单id
    private String pId;      // 产品id
    private int amount;       // 产品数量
    private String pname;     // 产品名称
    private String flag;      // 表的标记

    public TableBean() {
        super();
    }

    public TableBean(String orderId, String pId, int amount, String pname, String flag) {
        super();
        this.orderId = orderId;
        this.pId = pId;
        this.amount = amount;
        this.pname = pname;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId=in.readUTF();
        pId=in.readUTF();
        amount=in.readInt();
        pname=in.readUTF();
        flag=in.readUTF();
    }

    @Override
    public String toString() {
        return orderId +
                "\t" + amount +
                "\t" + pname ;
    }
}
