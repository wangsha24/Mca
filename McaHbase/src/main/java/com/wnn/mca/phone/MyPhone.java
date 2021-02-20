package com.wnn.mca.phone;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

//从hbase读取数据存入hdfs
public class MyPhone {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("mapreduce.app-submission.cross-platform","true");
        config.set("mapreduce.framework.name","yarn");
        config.set("hbase.zookeeper.quorum","hadoop52,hadoop53,hadoop54");

        GenericOptionsParser parser = new GenericOptionsParser(config, args);
        final Job job = Job.getInstance(config);
        job.setJobName("hbase to hdfs");
        job.setJarByClass(MyPhone.class);
        job.setJar("D:\\onlineEducation\\Mca\\McaHbase\\target\\McaHbase-1.0-SNAPSHOT.jar");

        Scan scan = new Scan();
//        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
//        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // set other scan attrs
        scan.addFamily(Bytes.toBytes("wordcount"));

        TableMapReduceUtil.initTableMapperJob(
                "wc",        // input table
                scan,               // Scan instance to control CF and attribute selection
                MyMapper.class,     // mapper class
                Text.class,         // mapper output key
                IntWritable.class,  // mapper output value
                job,true);

        job.setReducerClass(MyPhoneReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job,new Path("/wc.txt"));

        job.waitForCompletion(true);
        System.out.println("----over------");
    }
}
