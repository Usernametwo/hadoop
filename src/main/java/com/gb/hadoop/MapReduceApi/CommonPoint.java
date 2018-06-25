package com.gb.hadoop.MapReduceApi;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * input format:
 * 	a:b c 
 * 	b:a c
 * 
 * output format:
 * 	a-c: b
 * 	b-c: a
 * */
public class CommonPoint {
	public static class CMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] strs = value.toString().split(":");
			String[] friends = strs[1].toString().split(" ");
			Arrays.sort(friends);
			for(int i = 0 ; i < friends.length ; i ++) {
				for(int j = i+1 ; j < friends.length ; j ++) {
					context.write(new Text(friends[i] + "-" + friends[j]), new Text(strs[0]));
				}
			}
		}
	}
	
	public static class CReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text val : vals) {
				sb.append(val.toString());
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length != 2) {
			System.out.println("Usage CommonPoint <src_file> <output_dir>");
			System.exit(0);
		}
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)) {
			fs.delete(output, true);
		}
		
		Job job = Job.getInstance();
		job.setJobName("CommonPoint");
		job.setJarByClass(CommonPoint.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CMapper.class);
		job.setReducerClass(CReducer.class);
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
}
