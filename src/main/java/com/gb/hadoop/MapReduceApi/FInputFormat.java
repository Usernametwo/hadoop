package com.gb.hadoop.MapReduceApi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FInputFormat extends FileInputFormat<NullWritable, Text> {
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return super.isSplitable(context, filename);
	}

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		FRecordReader recordReader = new FRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}
}
