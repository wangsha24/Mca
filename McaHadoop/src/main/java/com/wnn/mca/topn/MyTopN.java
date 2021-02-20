package com.wnn.mca.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;

public class MyTopN {

    public static void main(String[] args) throws Exception {

        final Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("fs.defaultFS", "hdfs://hadoop52:8020");
        final GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        final String[] op = parser.getRemainingArgs();

        final Job job = Job.getInstance(conf);
        job.setJar("D:\\onlineEducation\\McaHadoop\\target\\McaHadoop-1.0-SNAPSHOT.jar");
        job.setJobName("My TOP");
        job.setJarByClass(MyTopN.class);

//        input
        TextInputFormat.addInputPath(job,new Path(op[0]));

        final Path path = new Path(op[1]);
        if(path.getFileSystem(conf).exists(path)){
            path.getFileSystem(conf).delete(path,true);
        }
        TextOutputFormat.setOutputPath(job,path);

//        map端join操作
        job.addCacheFile(new Path("/location.txt").toUri());

//        job.setNumReduceTasks(1);
//map
//        input
//        map
//        partitioner
//        sortComparator
//        combine优化

//        reduce
//        groupComparator

        job.setMapperClass(MyTopNMapper.class);
        job.setMapOutputKeyClass(KeyTopn.class);
        job.setMapOutputValueClass(IntWritable.class);

//map输出到缓冲区之间有分区器(将map输出的kv ,变成kvp)
        job.setPartitionerClass(MyTopNPartitioner.class);
        //排序是快排不需要管，排序比较器可以干预
        job.setSortComparatorClass(MyTopNSortComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setReducerClass(MyTopNReduce.class);
        job.setGroupingComparatorClass(MyTopNGroupingComparator.class);

        job.waitForCompletion(true);

        System.out.println("--over---");

    }
}
