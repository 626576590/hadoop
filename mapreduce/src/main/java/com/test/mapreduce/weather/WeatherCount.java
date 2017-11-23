package com.test.mapreduce.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.test.mapreduce.writable.WeatherWritable;

/**
 * weather 统计天气信息
 * 数据：
 *  1999-10-01 14:21:02    34c
 *  1999-11-02 13:01:02    30c
 *
 * 要求：
 * 将每年的每月中气温排名前三的数据找出来
 *
 * 实现：
 * 1.每一年用一个reduce任务处理;
 * 2.创建自定义数据类型，存储 [年-月-日-温度];
 * 2.自己实现排序函数 根据 [年-月-温度] 降序排列，也可以在定义数据类型中进行排序;
 * 3.自己实现分组函数，对 [年-月] 分组，reduce的key是分组结果，根据相同的分组值，统计reduce的value值，只统计三个值就可以，因为已经实现了自排序函数。
 *
 */
public class WeatherCount extends Configured implements Tool {
	
	private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	static class WeatherMapper extends
			Mapper<LongWritable, Text, WeatherWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String arr[] = line.split("\t");
			
			try {
				Date date = sdf.parse(arr[0]);
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				int year = c.get(Calendar.YEAR);
				int month = c.get(Calendar.MONTH)+1;
				int day = c.get(Calendar.DAY_OF_MONTH);
				double temperature = Double.parseDouble(arr[1].substring(0,arr[1].length()-1));
				WeatherWritable ww = new WeatherWritable();
				ww.setYear(year).setMonth(month).setDay(day).setTemperature(temperature);
				context.write(ww, value);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	static class WeatherReduce extends
	Reducer<WeatherWritable, Text, Text, NullWritable> {
		@Override
		protected void reduce(WeatherWritable keys, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int i=0;
			for (Text text : values) {
				i++;
				if(i>3)break;
				context.write(text, NullWritable.get());
			}
			
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.106.128:9000");
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "yearTemperature");
		job.setJarByClass(WeatherCount.class);

		job.setMapperClass(WeatherMapper.class);
		job.setMapOutputKeyClass(WeatherWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(WeatherReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		if (args.length != 2) {
			args = new String[] { "/year.txt",
					"/year01" };
		}
		
		job.setPartitionerClass(YearPartition.class);
         //根据年份创建指定数量的reduce task
             job.setNumReduceTasks(3);
 
             //设置排序 继承 WritableComparator
         //job.setSortComparatorClass(SortComparator.class);
 
             //设置分组 继承 WritableComparator 对reduce中的key进行分组
         job.setGroupingComparatorClass(GroupComparator.class);
		 
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path path = new Path(args[1]);
		//如果目录存在，则删除目录
		if(fs.exists(path)){
			 fs.delete(path,true);
		}
		FileOutputFormat.setOutputPath(job, path);

		// 提交job
		boolean bool = job.waitForCompletion(true);
		return bool ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WeatherCount(), args);
        System.exit(exitCode);
	}
}
