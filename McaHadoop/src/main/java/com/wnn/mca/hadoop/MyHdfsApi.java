package com.wnn.mca.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;

public class MyHdfsApi {
    FileSystem fs = null;

    @Before
    public void init(){
        try {
            Configuration conf = new Configuration(true);
            final URI uri = new URI("hdfs://mycluster");
            fs = FileSystem.get(uri,conf,"wnn");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testmkdir() throws Exception {

        final Path path = new Path("/test/win");
        final boolean flag = fs.mkdirs(path);
        System.out.println(flag);
    }

    @Test
    public void testopen() throws Exception {
        final Path path = new Path("/test/abc/data.txt");
        final FSDataInputStream in = fs.open(path);

        final FileOutputStream out = new FileOutputStream("D:/aa.txt");
        IOUtils.copyBytes(in,out,1024,true);

    }

    @Test
    public void testupdate() throws Exception {
        final Path src = new Path("d:/202007myplan.txt");
        final Path dst = new Path("/test");

        fs.copyFromLocalFile(src,dst);
    }

    @Test
    public void testlistFiles() throws Exception {

        final RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path("/test"), true);
        while (itr.hasNext()){
            final LocatedFileStatus locatedFileStatus = itr.next();
            System.out.println(locatedFileStatus.isDirectory());
            System.out.println(locatedFileStatus.isFile());
            System.out.println(locatedFileStatus.getPath().toString());
            System.out.println(Arrays.toString(locatedFileStatus.getBlockLocations()));
            System.out.println(locatedFileStatus.getLen());
            System.out.println("--------------------------");
            final BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for(BlockLocation block:blockLocations){
                System.out.println(Arrays.toString(block.getHosts()));
                System.out.println(block.getLength());
                System.out.println(Arrays.toString(block.getNames()));
                System.out.println(block.getOffset());

            }

            System.out.println("********************");
        }

    }
    @Test
    public void testcopyblock() throws Exception {

        final FileStatus fileStatus = fs.getFileStatus(new Path("/test/abc/data.txt"));
        System.out.println(fileStatus.toString());
        System.out.println(fileStatus.getBlockSize());
        System.out.println(fileStatus.getOwner());
        System.out.println(fileStatus.getGroup());
        System.out.println(fileStatus.getReplication());
        System.out.println(fileStatus.getLen());


    }



        @After
    public void destory() throws IOException {
        System.out.println("---over-----------");
        fs.close();
    }
}
