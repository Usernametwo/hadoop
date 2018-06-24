package com.gb.hadoop.MapReduceApi;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowWritable>{
	
	public static HashMap<String, Integer> category = new HashMap<String, Integer>();
	static {
		//0-index
		category.put("1", 0);
		category.put("2", 1);
	}
	@Override
	public int getPartition(Text key, FlowWritable value, int reduceNum) {
		// TODO Auto-generated method stub
//		return key.toString().hashCode() % reduceNum;
		return ((category.get(key.toString()) == null) ? 2 : category.get(key.toString()));
	}
	
}
