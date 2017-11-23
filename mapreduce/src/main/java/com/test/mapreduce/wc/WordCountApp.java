package com.test.mapreduce.wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountApp {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Text text = new Text();
		private IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//获取每一行的数据：hello a
			String line = value.toString();
			//通过空格来分割:hello,a
			String[] words = line.split(" ");
			//遍历输出
			//hello,1
			//a,1
			/*for (String w : words) {
				text.set(w);
				context.write(text, one);
			}*/
			
			StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	text.set(itr.nextToken());
            	context.write(text, one);
            }

		}
	}

	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sum = new IntWritable();
		@Override
		protected void reduce(Text keys, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			Integer count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			sum.set(count);
			context.write(keys, sum);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		if(args.length < 2){
			args = new String[]{
					"hdfs://192.168.106.128:9000/words",
					"hdfs://192.168.106.128:9000/out02"
			};
		}
		
		//创建配置对象
		Configuration conf = new Configuration();
		//创建job对象
		Job job = Job.getInstance(conf, "wordCount");
		//设置运行job的主类
		job.setJarByClass(WordCountApp.class);
		//设置mapper类
		job.setMapperClass(MyMapper.class);
		//设置reduce类
		job.setReducerClass(MyReduce.class);
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
