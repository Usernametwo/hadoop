package com.gb.hadoop.MapReduceApi;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/*
 * file format 
 * a b cd e cd e
 * 
 * output format
 * a 1
 * b 1
 * cd 2
 * e 2
 * */
public class WordCount {
	//static necessary
	public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			IntWritable iw = new IntWritable(1);
			for(String str:strs) {
				context.write(new Text(str), iw);
			}
		}
	}
	//static necessary
	public static class WCReduce extends Reducer<Text, IntWritable, NullWritable, Text> {
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable num:value) {
				sum = sum + num.get();
			}
			String ss = key.toString() + "  " + sum;
			context.write(NullWritable.get(), new Text(ss));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length  != 2) {
			System.out.println("Usage WordCount <inputpath> <outputpath>");
			System.exit(0);
		} 
		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//if dir exists , delete it
		if(fs.exists(outputpath)) {
			fs.delete(outputpath, true);
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName("wordcount");
		job.setJarByClass(WordCount.class);
		
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReduce.class);
//		job.setCombinerClass(WCReduce.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.waitForCompletion(true);
	}
}
