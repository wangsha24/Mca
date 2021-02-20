package com.wnn.mca.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v=new IntWritable();

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

        final String[] names = value.toString().split(" ");
        for (int i = 1; i < names.length ; i++) {
            final String direct = compare(names[0], names[i]);
            k.set(direct);
            v.set(0);
            context.write(k,v);
            for (int j = i+1; j < names.length ; j++) {
                final String indirect = compare(names[i], names[j]);
                k.set(indirect);
                v.set(1);
                context.write(k,v);
            }

        }
    }

    public static String compare(String s1,String s2){
        if(s1.compareTo(s2)>0){
            return s1+"-"+s2;
        }else{
            return s2+"-"+s1;
        }
    }
}
