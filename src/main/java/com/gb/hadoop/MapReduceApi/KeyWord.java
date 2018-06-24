package com.gb.hadoop.MapReduceApi;


import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/*
 * input format:
 * 	file1:
 * 		a b c
 * 	file 2:
 * 		a b c
 * 
 * output:
 * 	a file1:1,file2:1,
 * 	b file1:1,file2:1,
 * */
public class KeyWord {
	public static class KMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//get filename
			FileSplit fs = (FileSplit) context.getInputSplit();
			String filename = fs.getPath().getName();
			
			String[] vals = value.toString().replaceAll(",", " ").split(" ");
			for(String val : vals) {
				context.write(new Text(val.toLowerCase()), new Text(filename));
			}
		}
	}
	
	public static class KReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException{
			HashMap<String, Integer> hs = new HashMap<String, Integer>();
			for(Text val : vals) {
				String filename = val.toString();
				if(hs.get(filename) != null) {
					hs.put( filename, hs.get(filename) + 1 );
				} else {
					hs.put(filename, 1);
				}
			}
			Set<String> fileNames = hs.keySet();
			String result = "";
			for(String filename : fileNames) {
				result = result + filename + " : " + hs.get(filename) + ",";
			}
			context.write(key, new Text(result));
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException { 
		if(args.length  != 2) {
			System.out.println("Usage KeyWord <inputpath> <outputpath>");
			System.exit(0);
		}
		Path[] inputPahts = new Path[args.length-1];
		for(int i = 0 ; i < args.length - 1 ; i ++) {
			Path inputPath= new Path(args[i]);
			inputPahts[i] = inputPath;
		}
		Path outputpath = new Path(args[args.length-1]);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//if output dir exists , delete it
		if(fs.exists(outputpath)) {
			fs.delete(outputpath, true);
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName("keyword");
		job.setJarByClass(KeyWord.class);
		
		//set input and output 
		FileInputFormat.setInputPaths(job, inputPahts);
		FileOutputFormat.setOutputPath(job, outputpath);

		//set data format
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set mapper and reducer class
		job.setMapperClass(KMapper.class);
		job.setReducerClass(KReducer.class);
		
		//job.setCombinerClass(WCReduce.class);
		//set output foramt
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.waitForCompletion(true);
	}
}
