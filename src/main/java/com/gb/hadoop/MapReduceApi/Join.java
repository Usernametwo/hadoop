package com.gb.hadoop.MapReduceApi;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * input file format:
 * 	order:
 * 		a b
 * 		c d
 * 	info:
 * 		b apple
 * 		d perl
 * 
 * 	output file format:
 * 		a b apple
 * 		c d perl
 * */

public class Join {
	public static class JMapper extends Mapper<LongWritable, Text, Text, JInfoBean> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fs = (FileSplit) context.getInputSplit();
			String filename = fs.getPath().getName();
			JInfoBean jb = new JInfoBean();
			if(filename.equals("order")) {
				String[] order = value.toString().split(" ");
				jb.setOrderId(order[0]);
				jb.setpId(order[1]);
				jb.setFlag("order");
			} else if(filename.equals("info")) {
				String[] info = value.toString().split(" ");
				jb.setpId(info[0]);
				jb.setName(info[1]);
				jb.setFlag("info");
			}
			context.write(new Text(jb.getpId()), jb);
		}
	}
	
	public static class JReducer extends Reducer<Text, JInfoBean, JInfoBean, NullWritable> {
		public void reduce(Text key, Iterable<JInfoBean> values, Context context) throws IOException, InterruptedException {
			JInfoBean jb = new JInfoBean();
			for(JInfoBean val:values) {
				if(val.getFlag().equals("order")) {
					jb.setOrderId(val.getOrderId());
					jb.setpId(val.getpId());
				} else if(val.getFlag().equals("info")) {
					jb.setpId(val.getpId());
					jb.setName(val.getName());
				}
			}
			context.write(jb, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length != 3) {
			System.out.println("Usage Join <order> <info> <output_dir>");
			System.exit(0);
		}
		Path input_order = new Path(args[0]);
		Path input_info = new Path(args[1]);
		Path output = new Path(args[2]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(output)) {
			fs.delete(output, true);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(Join.class);
		job.setJobName("Join");
		
		job.setOutputKeyClass(JInfoBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JInfoBean.class);
		
		job.setMapperClass(JMapper.class);
		job.setReducerClass(JReducer.class);
		
		FileInputFormat.setInputPaths(job, input_order, input_info);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
}
