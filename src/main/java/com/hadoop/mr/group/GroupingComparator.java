package com.hadoop.mr.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

    protected GroupingComparator(){
        super(GroupBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //按照只要id相同就认为是相同的key
        GroupBean bean1 = (GroupBean) a;
        GroupBean bean2 = (GroupBean) b;
        return Integer.compare(bean1.getId(), bean2.getId());
    }
}
