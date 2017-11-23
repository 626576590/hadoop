package com.test.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.test.mapreduce.writable.UserWritable;
/**
 * 测试数据
 * 用户id   	收入		支出
 * 1      	1000 	0 
 * 2  		500 	300
 * 1  		2000 	1000
 * 2  		500 	200
 * 需求 
 * 用户id  	总收入       	总支出     	总余额
 * 1      	3000  	1000  	2000
 * 2		1000	500		500
 * @author zxt
 *
 */
public class MR {
	
	public static class MRMapper extends Mapper<LongWritable, Text, IntWritable,UserWritable >{
		private UserWritable userWritable = new UserWritable();
		private IntWritable intWritable = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			//获取每行数据
			String line = value.toString();
			//解析数据
			String words[] =line.split("\t");
			if(words.length==3){
				userWritable.setId(Integer.parseInt(words[0]))
				.setIncome(Integer.parseInt(words[1]))
				.setExpenses(Integer.parseInt(words[2]))
				.setSum(Integer.parseInt(words[1])-Integer.parseInt(words[2]));
				intWritable.set(Integer.parseInt(words[0]));
			}
			//写入
			context.write(intWritable, userWritable);
		}
	}
	
	public static class MRReduce extends Reducer<IntWritable,UserWritable, UserWritable, NullWritable>{
		
		/**
		 * 输入数据
		 * <1,{user01{1,1000,0,1000},user01{1,2000,1000,1000}}>
		 */
		
		private UserWritable userWritable = new UserWritable();
		private NullWritable nullWritable = NullWritable.get();
		
		@Override
		protected void reduce(IntWritable key, Iterable<UserWritable> values,
				Reducer<IntWritable,UserWritable, UserWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//初始化 收入，支出，总额
			Integer income = 0;
			Integer expenses = 0;
			
			for (UserWritable u : values) {
				income +=u.getIncome();
				expenses +=u.getExpenses();
			}
			Integer sum = income-expenses;
			//封装到对象中 
			userWritable.setId(key.get())
			.setIncome(income)
			.setExpenses(expenses)
			.setSum(sum);
			context.write(userWritable, nullWritable);
		}
	}

	public static void main(String[] args)throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "mr");
		job.setJarByClass(MR.class);
		
		job.setMapperClass(MRMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(UserWritable.class);
		
		job.setReducerClass(MRReduce.class);
		job.setOutputKeyClass(UserWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		args = new String[]{
				"hdfs://192.168.106.128:9000/countMR",
				"hdfs://192.168.106.128:9000/mr01"
		};
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		//提交job
		boolean bool = job.waitForCompletion(true);
		if(!bool){
			System.out.println("task is failure!");
		}
	}
	
}
