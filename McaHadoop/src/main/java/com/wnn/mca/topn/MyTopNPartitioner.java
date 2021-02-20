package com.wnn.mca.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyTopNPartitioner extends Partitioner<KeyTopn, IntWritable> {
    @Override
    public int getPartition(final KeyTopn keyTopn, final IntWritable v, final int numPartitions) {

        final int year = keyTopn.getYear();
        final int month = keyTopn.getMonth();

        return (year+month)%numPartitions;//容易造成数据倾斜
    }


}
