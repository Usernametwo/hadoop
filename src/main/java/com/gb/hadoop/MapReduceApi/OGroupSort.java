package com.gb.hadoop.MapReduceApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * input format:
 * 	3|2
 * 	2|1
 * `3|1
 * 	1|2
 * 
 * output format:
 * 	1|2
 * 	2|1
 * 	3|1
 * 	3|2
 * */

public class OGroupSort {

	public static class OMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
		OrderBean bean = new OrderBean();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] strs = value.toString().split("\\|");
			if(strs.length == 2) {
				bean.setItemid(new Text(strs[0]));
				bean.setAmount(new DoubleWritable(Double.parseDouble(strs[1])));
				context.write(bean, NullWritable.get());
			}
		}
	}
	
	public static class OReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
		OrderBean bean = new OrderBean();
		public void reduce(OrderBean key, NullWritable value, Context context) throws IOException, InterruptedException {
			context.write(key,NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length<2){
			System.out.println("Usage:<inputPath><outputPath>");
			System.exit(0);
		}
		Path inputPath = new Path(args[0]);
		Path OutputPath = new Path(args[1]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(OutputPath)){
			fs.delete(OutputPath,true);
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName("OGroupSort");
		job.setJarByClass(OGroupSort.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, OutputPath);
		
		job.setMapperClass(OMapper.class);
		job.setReducerClass(OReducer.class);
		
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		//job.setPartitionerClass(MyPartitioner.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(OPartitioner.class);
		
		job.setGroupingComparatorClass(OGroupComparator.class);
		
		job.waitForCompletion(true);
	}
}
