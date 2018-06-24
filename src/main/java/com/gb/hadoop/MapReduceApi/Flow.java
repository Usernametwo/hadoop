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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/*
 * file format
 * phoneid|uploadFlow|downloadFlow 
 * 
 * output format
 * phoneid|uploadFlow:**|downloadFlow:**|totalFlow:**
 * */
public class Flow {	
	public static class FMapper extends Mapper<LongWritable, Text, Text, FlowWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\\|");
			String phoneid = strs[0];
			long upload_flow = Long.parseLong(strs[1]);
			long download_flow = Long.parseLong(strs[2]);
//			long total_flow = upload_flow + download_flow;
			FlowWritable fw = new FlowWritable(upload_flow, download_flow);
			context.write(new Text(phoneid), fw);
		}
	}
	
	public static class Freduce extends Reducer<Text, FlowWritable, Text, Text> {
		public void reduce(Text key, Iterable<FlowWritable> value, Context context) throws IOException, InterruptedException {
			long upload_flow = 0;
			long download_flow = 0;
			long total_flow = 0;
			for(FlowWritable val:value) {
				upload_flow = upload_flow + val.getUpFlow();
				download_flow = download_flow + val.getDownFlow();
				total_flow = total_flow + val.getTotalFlow();
			}
			context.write(key, new Text("uploadFlow:" + upload_flow + "downloadFlow:" + download_flow + "totalFlow:" + total_flow));
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
		job.setJobName("flow");
		job.setJarByClass(Flow.class);
		
		//set input and output 
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);

		//set data format
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set mapper and reducer class
		job.setMapperClass(FMapper.class);
		job.setReducerClass(Freduce.class);
		
		//set task partition
		job.setNumReduceTasks(3);
		job.setPartitionerClass(FlowPartitioner.class);
		
		//job.setCombinerClass(WCReduce.class);
		//set output foramt
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
}
