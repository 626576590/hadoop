package com.test.mapreduce.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.test.mapreduce.writable.WeatherWritable;

/**
 *
 * 创建分区，通过key中的year来创建分区
 *
 * Created by Edward on 2016/7/11.
 */
 public class YearPartition extends HashPartitioner <WeatherWritable, Text>{
     @Override
     public int getPartition(WeatherWritable key, Text value, int numReduceTasks) {
         return key.getYear()%numReduceTasks;
     }
 }
