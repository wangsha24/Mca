package com.wnn.mca.mrhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

public class MyMrHbase {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("mapreduce.app-submission.cross-platform","true");
        config.set("mapreduce.framework.name","local");
        config.set("hbase.zookeeper.quorum","hadoop52,hadoop53,hadoop54");

        Job job = Job.getInstance(config);
        job.setJarByClass(MyMrHbase.class);     // class that contains mapper


        String startrow = "18901070303_"+(Long.MAX_VALUE-format.parse("20200727000000").getTime());
        String stoprow = "18901070303_"+(Long.MAX_VALUE-format.parse("20200701000000").getTime());
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startrow));
        scan.withStopRow(Bytes.toBytes(stoprow));
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // set other scan attrs


        TableMapReduceUtil.initTableMapperJob(
                "psn",      // input table
                scan,             // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "psn2",      // output table
                null,             // reducer class
                job);
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

    }

    static Random random = new Random();
    static SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
    public static String getNumber(String str){
        return str+String.format("%08d",random.nextInt(99999999));
    }
    public static String getDate(String str){
        return str+String.format("%02d%02d%02d%02d%02d",
                random.nextInt(12)+1,random.nextInt(30)+1,
                random.nextInt(24)+1,random.nextInt(60)+1,random.nextInt(60)+1);
    }
}
