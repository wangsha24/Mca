package com.wnn.mca.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

public class ApiTest {
    Connection connection = null;
    TableName psn = TableName.valueOf("psn");
    Admin admin = null;//DDL
    Table table = null;//DML

    @Before
    public void init() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop52,hadoop53,hadoop54");

        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        table = connection.getTable(psn);
    }

    @Test
    public void createTable() throws Exception {


            final HTableDescriptor table = new HTableDescriptor(psn);
            table.addFamily(new HColumnDescriptor("myinfo"));

            if(admin.tableExists(table.getTableName())){
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);

    }

    @Test
    public void scan() throws Exception {//18927159750_9223370441016113807
        String startrow = "18901070303_"+(Long.MAX_VALUE-format.parse("20200727000000").getTime());
        String stoprow = "18901070303_"+(Long.MAX_VALUE-format.parse("20200701000000").getTime());
        final Scan scan = new Scan();
//        scan.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("celled"));
        scan.withStartRow(Bytes.toBytes(startrow));
        scan.withStopRow(Bytes.toBytes(stoprow));
        final ResultScanner rs = table.getScanner(scan);
//        ResultScanner rs = table.getScanner(scan);
//        for (Result result = rs.next();result != null; result = rs.next()) {
//              final String calling = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("celling"))));
//            final String called = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("celled"))));
//            final String length = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("length"))));
//            final String type = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("type"))));
//            final String date = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("date"))));
//
//            System.out.print(calling+" ");
//            System.out.print(called+" ");
//            System.out.print(length+" ");
//            System.out.print(date+" ");
//            System.out.println(type);
//        }

        final Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()){
            final Result result = iterator.next();

            final String calling = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("celling"))));
            final String called = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("celled"))));
            final String length = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("length"))));
            final String type = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("type"))));
            final String date = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("date"))));

            System.out.print(calling+" ");
            System.out.print(called+" ");
            System.out.print(length+" ");
            System.out.print(date+" ");
            System.out.println(type);

        }
    }

    @Test
    public void get() throws IOException {
        final Get get = new Get(Bytes.toBytes("18901070303_9223370440931145807"));
        get.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("celling"));
        get.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("celled"));
        get.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("type"));
        get.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("length"));
        get.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("date"));
        final Result result = table.get(get);
        final Cell cell = result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("celling"));
        final String calling = Bytes.toString(CellUtil.cloneValue(cell));
        final String called = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("celled"))));
        final String length = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("length"))));
        final String type = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("type"))));
        final String date = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("date"))));

        System.out.println(calling);
        System.out.println(called);
        System.out.println(length);
        System.out.println(date);
        System.out.println(type);

    }

    @Test//18975457019_9223370427580746807
    public void scanproto() throws Exception {//18927159750_9223370441016113807
        String startrow = "18975457019_"+(Long.MAX_VALUE-format.parse("20200727000000").getTime());
        String stoprow = "18975457019_"+(Long.MAX_VALUE-format.parse("20200701000000").getTime());
        final Scan scan = new Scan();
//        scan.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("celled"));
        scan.withStartRow(Bytes.toBytes(startrow));
        scan.withStopRow(Bytes.toBytes(stoprow));
        final ResultScanner rs = table.getScanner(scan);

        final Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()){
            final Result result = iterator.next();

            final Cell cell = result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("phone"));
            final byte[] row = CellUtil.cloneValue(cell);

            final Phone.phone phone = Phone.phone.parseFrom(row);
            System.out.println(phone);
            System.out.println(phone.getCelling());
            System.out.println(phone.getCelled());
            System.out.println(phone.getType());
            System.out.println(phone.getDate());

        }
    }

    @Test
    public void putProto() throws Exception {
        final ArrayList<Put> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String number = getNumber("189");
            for (int j = 0; j < 10000; j++) {
                final String bphone = getNumber("153");
                final String date = getDate("2020");
                String length = String.valueOf(random.nextInt(100));
                String type = String.valueOf(random.nextInt(2));
                String rowkey = number + "_" + (Long.MAX_VALUE - format.parse(date).getTime());

                final Phone.phone.Builder builder = Phone.phone.newBuilder();
                builder.setCelling(number);
                builder.setCelled(bphone);
                builder.setDate(date);
                builder.setType(type);
                builder.setLength(length);

                final Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("phone"),builder.build().toByteArray());

                list.add(put);
            }
        }
        table.put(list);
    }

    @Test
    public void getproto() throws IOException {
        final Get get = new Get(Bytes.toBytes("18982088413_9223370456003368807"));
        final Result result = table.get(get);
        final Cell cell = result.getColumnLatestCell(Bytes.toBytes("myinfo"), Bytes.toBytes("phone"));
        final byte[] row = CellUtil.cloneValue(cell);

        final Phone.phone phone = Phone.phone.parseFrom(row);
        System.out.println(phone);
        System.out.println(phone.getCelling());
        System.out.println(phone.getCelled());
        System.out.println(phone.getType());
        System.out.println(phone.getDate());

    }


    @Test
    public void put() throws Exception{
        final ArrayList<Put> list = new ArrayList<>();
        for(int i=0; i<10;i++){
            final String number = getNumber("189");
            for(int j=0;j<10000;j++){
                final String bphone = getNumber("153");
                final String date = getDate("2020");
                String length = String.valueOf(random.nextInt(100));
                String type = String.valueOf(random.nextInt(2));

                String rowkey = number+"_"+(Long.MAX_VALUE-format.parse(date).getTime());
                final Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn("myinfo".getBytes(),"celling".getBytes(),number.getBytes());
                put.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("celled"),Bytes.toBytes(bphone));
                put.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("length"),Bytes.toBytes(length));
                put.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("date"),Bytes.toBytes(date));
                put.addColumn(Bytes.toBytes("myinfo"),Bytes.toBytes("type"),Bytes.toBytes(type));

                list.add(put);
            }
        }
        table.put(list);
    }

    Random random = new Random();
    SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
    public String getNumber(String str){
        return str+String.format("%08d",random.nextInt(99999999));
    }
    public String getDate(String str){
        return str+String.format("%02d%02d%02d%02d%02d",
                random.nextInt(12)+1,random.nextInt(30)+1,
                random.nextInt(24)+1,random.nextInt(60)+1,random.nextInt(60)+1);
    }

    public static void main(String[] args) throws ParseException {
        final ApiTest apiTest = new ApiTest();
//        System.out.println(Bytes.toBytes("20200701000000").toString());
//        System.out.println(apiTest.getNumber("158"));

        final Date parse = apiTest.format.parse("9223370427580746807l");
        System.out.println(apiTest.format.format(parse));
    }

    @After
    public void destory(){
        try {
            admin.close();
            table.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
