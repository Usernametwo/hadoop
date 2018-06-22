package com.gb.hadoop.MapReduceApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/*
 * file format
 * 	1 ABC
 * 	2 A
 * 	3 B
 * 
 * output format
 * 	A 12 2
 * 	B 13 2
 * 	C 1 1
 * */
public class homework {
	public static class HMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] results = value.toString().split(" ");
			for(int i = 0 ; i < results[1].length() ; i ++) {
				context.write(new Text(results[1].substring(i, i+1)), new Text(results[0]));
			}
		}
	}
	
	public static class HReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			int length = 0;
			String result = "";
//			for(Text val : vals) {
//				result += val.toString();
//				length ++;
//			}
			ArrayList<Integer> al = new ArrayList<Integer>();
			for(Text val : vals) {
				al.add(Integer.valueOf(val.toString()));
				length++;
			}
			Collections.sort(al);
			for(Integer r:al) {
				result += r;
			}
			result = result + " " + length;
			context.write(new Text(key), new Text(result));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length != 2) {
			System.out.println("Usage homework <src_file> <output_dir>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Path d_path = new Path(args[1]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//check the output dir
		if(fs.exists(d_path)) {
			fs.delete(d_path, true);
		}
		
		Job job = Job.getInstance();
		job.setJobName("homework");
		job.setJarByClass(homework.class);
		FileInputFormat.setInputPaths(job, s_path);
		FileOutputFormat.setOutputPath(job, d_path);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(HMapper.class);
		job.setReducerClass(HReducer.class);
		
		job.waitForCompletion(true);
	}
}
