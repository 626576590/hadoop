package com.test.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * 统计小说人物出现次数
 * @author zxt
 *
 */
public class StoryNameCount {
	
	
	 public static Set<String> dic = new HashSet<>();
	 	//把需要统计的名字存放到set里面
	    static {
	        String ProjectPath = StoryNameCount.class.getResource("/").getFile().toString();
	        try {
	            BufferedReader br = new BufferedReader(new FileReader(new File(
	                    ProjectPath + File.separator + "\\DreamOfRed.txt")));
	            String line=null;
	            while((line=br.readLine())!=null){
	                line=line.replaceAll("\\s+", "");
	                dic.add(line);
	            }
	            br.close();
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	    
	    private static List<String> parse(String text){
	        List<String> words = new ArrayList<String>();
	        //创建IKAnalyzer中文分词对象  
	        IKAnalyzer analyzer = new IKAnalyzer();  
	        // 使用智能分词  
	        analyzer.setUseSmart(true);
	        // 分词
	        StringReader reader = new StringReader(text);
	        try {
	            TokenStream ts = analyzer.tokenStream("content",reader);
	            CharTermAttribute strs = ts.getAttribute(CharTermAttribute.class);
	            ts.addAttribute(CharTermAttribute.class); 
	            ts.reset();
	            // 遍历分词数据
	            while (ts.incrementToken()) {
	                if(dic.contains(strs.toString())){
	                    words.add(strs.toString());
	                }
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        reader.close();
	        return words;
	    }
	
	private static class DreamOfRedMansionMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> names = parse(value.toString());
            for (String name : names) {
                context.write(new Text(name), new LongWritable(1));
            }
        }
    }

    private static class DreamOfRedMansionReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
            Long sum = 0L;
            for (LongWritable value : values) {
                sum = sum + value.get();
            }
            context.write(new Text(key+","), new LongWritable(sum));
        }
    }

    public static class DreamOfRedMansionSortMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            LongWritable data = new LongWritable(Integer.parseInt(value.toString().split(",")[1].trim()));
            context.write(data, new Text(value.toString().split(",")[0]));
        }
        
    }
    
    public static class DreamOfRedMansionSortReduce extends Reducer<LongWritable, Text, Text, LongWritable>{

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values,
                Reducer<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for(Text str : values){
                context.write(str, key);
            }
        }
        
    }
		
	
	public static void main(String[] args)throws Exception {
		if(args.length < 2){
			args = new String[]{
					"hdfs://192.168.106.129:9000/test/story",
					"hdfs://192.168.106.129:9000/story01"
			};
		}
		
		Configuration cfg = new Configuration();
        Job job = Job.getInstance(cfg);
        job.setJobName("DreamOfRedMansion");
        job.setJarByClass(StoryNameCount.class);

        // mapper
        job.setMapperClass(DreamOfRedMansionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // reducer
        job.setReducerClass(DreamOfRedMansionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf);
        job2.setJobName("DreamOfRedMansionSort");
        job2.setJarByClass(StoryNameCount.class);

        // sortmapper
        job2.setMapperClass(DreamOfRedMansionSortMapper.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        // sortreducer
        job2.setReducerClass(DreamOfRedMansionSortReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.106.129:9000/red_Out_sort"));

        job2.waitForCompletion(true);
	}

}
