package com.test.mapreduce.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text keys, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		Integer count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(keys, new IntWritable(count));
	}
}
