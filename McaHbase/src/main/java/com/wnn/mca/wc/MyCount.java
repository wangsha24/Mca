package com.wnn.mca.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//从hdfs读出存入hbase(表必须已经存在)
public class MyCount {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("mapreduce.app-submission.cross-platform","true");
        config.set("mapreduce.framework.name","local");
        config.set("hbase.zookeeper.quorum","hadoop52,hadoop53,hadoop54");

        Job job = Job.getInstance(config);
        job.setJarByClass(MyCount.class);

        FileInputFormat.addInputPath(job,new Path("/friend.txt"));

        job.setMapperClass(MywcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        TableMapReduceUtil.initTableReducerJob(
                "wc",        // output table
                MyTableReducer.class,    // reducer class
                job,null,null,null,null,false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(1);   // at least one, adjust as required

        boolean b = job.waitForCompletion(true);
    }
}
