package com.wnn.mca.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class MyTopNReduce extends Reducer<KeyTopn, IntWritable, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable();
    private FileOutputStream fs = null;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        fs=new FileOutputStream("/home/atguigu/newReducetopn.txt");
    }

    @Override
    protected void reduce(final KeyTopn key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

        final Iterator<IntWritable> iterator = values.iterator();
        int flag = 0;
        int day= 0;

        while (iterator.hasNext()){
            iterator.next();
            fs.write(key.toString().getBytes());
            if(flag==0){
                 int wd = key.getWd();
                 day=key.getDay();
                 k.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay()+"-"+key.getLocation());
                 v.set(wd);
                flag++;
                 context.write(k,v);
            }

            if(flag!=0 && key.getDay()!=day){
                k.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay()+"-"+key.getLocation());
                v.set(key.getWd());
                context.write(k,v);
                break;
            }
        }

        fs.write("----------\n".getBytes());
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        fs.close();
    }
}
