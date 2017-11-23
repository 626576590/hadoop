package com.test.mapreduce.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.test.mapreduce.writable.WeatherWritable;

/**
  * 创建分组类，继承WritableComparator
  * 【年-月】
  * Created by Edward on 2016/7/11.
  */
public class GroupComparator extends WritableComparator {
	 GroupComparator()
    {
        super(WeatherWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherWritable ia = (WeatherWritable)a;
        WeatherWritable ib = (WeatherWritable)b;

        int result = Integer.compare(ia.getYear(),ib.getYear());
        if(result == 0)
        {
            return Integer.compare(ia.getMonth(),ib.getMonth());
        }
        else
            return result;
    }
}
