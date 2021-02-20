package com.wnn.mca.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

        int flag = 0;
        int sum =0 ;
        final Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            final IntWritable v = iterator.next();
            if(0==v.get()){
                flag=1;
                break;
            }
            sum+=v.get();

        }
        if(flag==0){
            context.write(key,new IntWritable(sum));
        }
    }
}
