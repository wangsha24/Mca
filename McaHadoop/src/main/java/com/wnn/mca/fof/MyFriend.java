package com.wnn.mca.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyFriend {

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration(true);
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.app-submission.cross-platform","true");

        final GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        final String[] ops = parser.getRemainingArgs();

        final Job job = Job.getInstance(conf);
        TextInputFormat.addInputPath(job,new Path(ops[0]));
        final Path path = new Path(ops[1]);
        if(path.getFileSystem(conf).exists(path))path.getFileSystem(conf).delete(path);
        TextOutputFormat.setOutputPath(job,path);
        job.setJar("D:\\onlineEducation\\McaHadoop\\target\\McaHadoop-1.0-SNAPSHOT.jar");

        job.setJobName("recommend");

        job.setMapperClass(FMapper.class);
        job.setReducerClass(FReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);

    }
}
