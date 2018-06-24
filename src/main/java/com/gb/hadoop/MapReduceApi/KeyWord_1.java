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
 * 	a file1:1,file2:1
 * 	b file1:1,file2:1
 * */
public class KeyWord_1 {
      public static class KeywordMap extends Mapper<LongWritable,Text,Text,Text>{
    	  Text keyWord;
    	  Text keyValue = new Text("1");
    	  protected void map(LongWritable key,Text value,Context context) 
    			  throws IOException,InterruptedException{
    		 FileSplit split = (FileSplit)context.getInputSplit();
    		 String fileName = split.getPath().getName();
    		 String valueStr = value.toString().replaceAll(","," ");
    		  String[] strs = valueStr.toString().split(" ");
    		 
    		  for(int i=0;i<strs.length;i++){
    			  keyWord = new Text(strs[i].toLowerCase()+"_"+fileName);
    				  context.write(keyWord,keyValue);}
    		  } 
    	}
      
      public static class keywordCombiner extends Reducer<Text,Text,Text,Text>{
    	  protected void reduce(Text key,Iterable<Text> values,Context context) 
    			  throws IOException,InterruptedException{
    		  String[] keyWord = key.toString().split("_");
    		  String word = keyWord[0];
    		  String fileName = keyWord[1];
    		  int sum = 0;
    		  for(Text it:values){
    			  sum+= Integer.parseInt(it.toString());
    		  }
    		  context.write(new Text(word),new Text(fileName +":"+sum));
    	  }
    	 
      }
      
      public static class KeywordReduce extends Reducer<Text,Text,Text,Text>{
    	  protected void reduce(Text key,Iterable<Text> values,Context context) 
    			  throws IOException,InterruptedException{
    		  StringBuffer sb = new StringBuffer();
    		  for(Text val:values){
    			  sb.append(val.toString());
    			  sb.append(",");
    		  }
    		  sb.deleteCharAt(sb.length() - 1);
    		  Text value = new Text(sb.toString());
    		  context.write(key,value);
    	  }
      }

      public static void main(String[] args)throws Exception{
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
		job.setJarByClass(KeyWord_1.class);
		
		//set input and output 
		FileInputFormat.setInputPaths(job, inputPahts);
		FileOutputFormat.setOutputPath(job, outputpath);

		//set data format
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set mapper , combiner and reducer class
        job.setMapperClass(KeywordMap.class);
        job.setCombinerClass(keywordCombiner.class);
        job.setReducerClass(KeywordReduce.class);
		
		//job.setCombinerClass(WCReduce.class);
		//set output foramt
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.waitForCompletion(true);
          
      }
}
