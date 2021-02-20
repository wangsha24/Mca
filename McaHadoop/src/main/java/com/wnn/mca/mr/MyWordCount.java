package com.wnn.mca.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyWordCount {

    public static void main(String[] args) {
        final Configuration conf = new Configuration(true);
        try {

            conf.set("mapreduce.app-submission.cross-platform","true");
            conf.set("mapreduce.framework.name","yarn");


            final GenericOptionsParser parser = new GenericOptionsParser(conf, args);
            final String[] op = parser.getRemainingArgs();

            final Job job = Job.getInstance(conf);
            job.setJarByClass(MyWordCount.class);
            job.setJobName("WordCount");
            job.setJar("D:\\onlineEducation\\McaHadoop\\target\\McaHadoop-1.0-SNAPSHOT.jar");

            FileInputFormat.addInputPath(job,new Path(op[0]));



            final Path outpath = new Path(op[1]);
            if(outpath.getFileSystem(conf).exists(outpath)){
                outpath.getFileSystem(conf).delete(outpath,true);
            }
            FileOutputFormat.setOutputPath(job,outpath);

            job.setMapperClass(MyWordCountMapper.class);
            job.setReducerClass(MyWordCountReducer.class);


            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

//            job.setNumReduceTasks(1);
            job.setCombinerClass(MyWordCountReducer.class);
            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
