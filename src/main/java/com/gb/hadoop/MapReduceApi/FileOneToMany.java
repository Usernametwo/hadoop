package com.gb.hadoop.MapReduceApi;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 *input format:
 *	a a a
 *	b b b
 * 	c c c
 * 
 * output format:
 * 	a-r-00000:
 * 		a a
 * 	b-r-00000:
 * 		b b
 * 	c-r-00000:
 * 		c c
 * 	
 * */
public class FileOneToMany {
	
	public static class FMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			context.write(new Text(strs[0] + " " + strs[1]), new Text(strs[2]));
		}
	}
	
	public static class FReducer extends Reducer<Text, Text, Text, Text> {
		
		private MultipleOutputs<Text, Text> multipleOutputs = null;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs.close();
			multipleOutputs = null;
		}
		
		//this must be Iterable<Text>
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] strs = key.toString().split(" ");
			for(Text val : values) {
				multipleOutputs.write(new Text(strs[0]), new Text(strs[1]), val.toString());
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path[] inputPaths = new Path[args.length - 1];
		for(int i = 0 ; i < args.length - 1 ; i++ ) {
			inputPaths[i] = new Path(args[i]);
		}
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(args[args.length - 1]);
		if(fs.exists(outputPath)) { 
			fs.delete(outputPath, true);
		}
		Job job = Job.getInstance();
		job.setJobName("FileOneToMany");
		job.setJarByClass(FileOneToMany.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FMapper.class);
		job.setReducerClass(FReducer.class);
		
//		MultipleOutputs.addNamedOutput(job, "a", TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "b", TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "c", TextOutputFormat.class, Text.class, Text.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		
	}
}
