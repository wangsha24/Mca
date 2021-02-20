package com.wnn.mca.topn;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyTopNGroupingComparator extends WritableComparator {

    public MyTopNGroupingComparator(){
        super(KeyTopn.class,true);
    }

    @Override
    public int compare(final WritableComparable a, final WritableComparable b) {
        KeyTopn o1 = (KeyTopn)a;
        KeyTopn o2 = (KeyTopn)b;

        final int c1 = Integer.compare(o1.getYear(), o2.getYear());
        if(c1==0){
            final int c2 = Integer.compare(o1.getMonth(), o2.getMonth());
            return c2;
        }
        return c1;
    }
}
