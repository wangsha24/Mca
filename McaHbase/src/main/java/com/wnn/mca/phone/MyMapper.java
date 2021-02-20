package com.wnn.mca.phone;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyMapper extends TableMapper<Text, IntWritable>  {
    public static final byte[] CF = "wordcount".getBytes();
    public static final byte[] ATTR1 = "count".getBytes();

    private  IntWritable v = new IntWritable(1);
    private Text k = new Text();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        final int num = Bytes.toInt(value.getValue(CF, ATTR1));
        v.set(num);

        final String word = Bytes.toString(row.get());
        k.set(word);
        System.out.println(k.toString()+"---"+v.toString());
        context.write(k, v);

    }
}