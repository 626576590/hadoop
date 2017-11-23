package com.test.mapreduce.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountMapReduce {

	
	public static void main(String[] args) throws Exception {
		
		if(args.length < 2){
			args = new String[]{
					"hdfs://192.168.106.128:9000/words",
					"hdfs://192.168.106.128:9000/out01"
			};
		}
		
		//创建配置对象
		Configuration conf = new Configuration();
		//创建job对象
		Job job = Job.getInstance(conf, "wordCount");
		//设置运行job的主类
		job.setJarByClass(WordCountMapReduce.class);
		//设置mapper类
		job.setMapperClass(WordCountMapper.class);
		//设置reduce类
		job.setReducerClass(WordCountReduce.class);
		//设置输出的key与value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//设置输入输出的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//提交job
		boolean bool = job.waitForCompletion(true);
		if(!bool){
			System.out.println("task is failure!");
		}
	}

}
