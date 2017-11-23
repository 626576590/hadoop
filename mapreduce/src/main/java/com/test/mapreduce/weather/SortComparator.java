package com.test.mapreduce.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.test.mapreduce.writable.WeatherWritable;

/**
5  * 排序类，继承WritableComparator
6  * 排序规则【年-月-温度】 温度降序
7  * Created by Edward on 2016/7/11.
8  */
public class SortComparator extends WritableComparator {
	 /**
      * 调用父类的构造函数
      */
     SortComparator()
     {
         super(WeatherWritable.class, true);
     }
 
 
     /**
      * 比较两个对象的大小，使用降序排列
      * @param a
      * @param b
      * @return
      */
     @Override
     public int compare(WritableComparable a, WritableComparable b) {
 
         WeatherWritable ia = (WeatherWritable)a;
         WeatherWritable ib = (WeatherWritable)b;
 
         int result = Integer.compare(ia.getYear(),ib.getYear());
         if(result == 0)
         {
             result = Integer.compare(ia.getMonth(),ib.getMonth());
             if(result == 0)
             {
                 return -Double.compare(ia.getTemperature(), ib.getTemperature());
             }
             else
                 return result;
         }
         else
             return result;
     }
 }
