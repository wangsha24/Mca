package com.wnn.mca.phone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyPhoneReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
        context.write(key,values.iterator().next());
    }
}
