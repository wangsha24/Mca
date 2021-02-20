package com.wnn.mca.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyTopn implements WritableComparable<KeyTopn> {
    private int year;
    private int month;
    private int day;
    private int wd;
    private String location;

    public String getLocation() {
        return location;
    }

    public void setLocation(final String location) {
        this.location = location;
    }

    public int getYear() {
        return year;
    }

    public void setYear(final int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(final int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(final int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(final int wd) {
        this.wd = wd;
    }

    @Override
    public String toString() {
        return "KeyTopn{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", wd=" + wd +
                ", location='" + location + '\'' +
                '}';
    }

    @Override
    public int compareTo(final KeyTopn that) {
        int c1 = Integer.compare(this.year, that.getYear());
        if(c1==0){
            int c2 = Integer.compare(this.month, that.getMonth());
            if(c2==0){
                return Integer.compare(this.day, that.getDay());
            }
            return c2;
        }
        return c1;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(wd);
        out.writeUTF(location);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.wd = in.readInt();
        this.location=in.readUTF();
    }
}
