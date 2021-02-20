package com.wnn.mca.wc;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Date;

public class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
    public static final byte[] CF = "wordcount".getBytes();
    public static final byte[] WORLD = "world".getBytes();
    public static final byte[] COUNT = "count".getBytes();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (IntWritable val : values) {
            i += val.get();
        }
        long id = Long.MAX_VALUE-System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(CF, WORLD, key.toString().getBytes());
        put.addColumn(CF, COUNT, Bytes.toBytes(i+"")); //不要把整型数字直接转成字节数组，而是先转成字符串再转字节数组

        context.write(null, put);
    }
}
