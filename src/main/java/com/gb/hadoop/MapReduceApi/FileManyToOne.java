package com.gb.hadoop.MapReduceApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * input format:
 * 	file1:
 * 		abc
 * 	file2:
 * 		abc
 * 	file3:
 * 		abc
 * 
 * output format
 * 	outputfile:
 * 		file1 abc
 * 		file2 abc
 * 		file3 abc
 * */
public class FileManyToOne {
	
	public static class FMapper extends Mapper<NullWritable, Text, Text, Text> {
		
		private Text filenameKey;
		protected void setup(Context context) {
			FileSplit split = (FileSplit) context.getInputSplit();
			String str = split.getPath().getName();
			filenameKey = new Text(str);
		}
		
		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(filenameKey, value);
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
		job.setJobName("FileManyToOne");
		job.setJarByClass(FileManyToOne.class);
		job.setInputFormatClass(FInputFormat.class);
		FileInputFormat.setInputPaths(job, inputPaths);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FMapper.class);
		
		job.waitForCompletion(true);
		
	}
}
