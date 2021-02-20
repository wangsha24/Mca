package com.wnn.mca.topn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.*;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

public class MyTopNMapper extends Mapper<LongWritable, Text,KeyTopn, IntWritable> {

    private KeyTopn k = new KeyTopn();
    private IntWritable v= new IntWritable();
    private FileOutputStream fs = null;
    private HashMap<String,String> map=new HashMap<>();

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        fs=new FileOutputStream("/home/atguigu/newMaptopn.txt");

        //map端join
        final URI[] cacheFiles = context.getCacheFiles();
        final Path path = new Path(cacheFiles[0].toString());

        fs.write((path.getName().toString()+"\nname\n").getBytes());
        fs.write((path.getParent().toString()+"\nparent\n").getBytes());

        final BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(path.getName())));
        String line = bufferedReader.readLine();

        while (line!=null){
            final String[] split = line.split("\t");
            map.put(split[0],split[1]);
            line=bufferedReader.readLine();
        }

    }



        @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        //2019-6-1 22:22:22 1 40
        final String s = value.toString();

        final String[] ss = s.split(" ");
        final String[] date = ss[0].split("-");
        k.setYear(Integer.parseInt(date[0]));
        k.setMonth(Integer.parseInt(date[1]));
        k.setDay(Integer.parseInt(date[2]));
        k.setWd(Integer.parseInt(ss[3]));
        k.setLocation(map.get(ss[2]));
        v.set(Integer.parseInt(ss[3]));
        fs.write(k.toString().getBytes());

        fs.write("\n----------\n".getBytes());
        context.write(k,v);

    }

//        @Override
//    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
//
//        final StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
//        final String ymd = stringTokenizer.nextToken();
//
//        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        try {
//            final Date date = simpleDateFormat.parse(ymd);
//            final Calendar calendar = Calendar.getInstance();
//            calendar.setTime(date);
//            k.setYear(calendar.get(Calendar.YEAR));
//            k.setMonth(calendar.get(Calendar.MONTH)+1);
//            k.setDay(calendar.get(Calendar.DAY_OF_MONTH));
//
//            stringTokenizer.nextToken();
//            stringTokenizer.nextToken();
//            final String wd = stringTokenizer.nextToken();
//            k.setWd(Integer.parseInt(wd));
//            fs.write((k.getYear()+"\n").getBytes());
//            context.write(k,v);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        fs.write("----------\n".getBytes());
//
//    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        fs.close();
    }
}
