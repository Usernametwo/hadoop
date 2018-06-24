package com.gb.hadoop.MapReduceApi;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OPartitioner extends Partitioner<OrderBean, NullWritable> {

	@Override
	public int getPartition(OrderBean bean, NullWritable value, int numReduceTasks) {
		// TODO Auto-generated method stub
		return ( bean.getItemid().hashCode()&Integer.MAX_VALUE) % numReduceTasks;
	}
	
}
