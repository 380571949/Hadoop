package com.hadoop.mr.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.ArrayList;

public class TableReduce extends Reducer<Text,TableBean,TableBean, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderList=new ArrayList<>();
        TableBean table=new TableBean();
        for(TableBean bean : values){
            if("order".equals(bean.getFlag())){
                TableBean tmp=new TableBean();
                try {
                    BeanUtils.copyProperties(tmp, bean);
                    orderList.add(tmp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    BeanUtils.copyProperties(table,bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        //拼接表
        for(TableBean order:orderList){
            order.setPname(table.getPname());
            context.write(order,NullWritable.get());
        }
    }
}
