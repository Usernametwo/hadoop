package com.gb.hadoop.MapReduceApi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * user-define Writable
 * */
public class FlowWritable implements Writable{
	
	private long upFlow;
	private long downFlow;
	private long totalFlow;
	
	public FlowWritable() {
		// TODO Auto-generated constructor stub
	}
	
	public FlowWritable(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalFlow = upFlow + downFlow;
	}
	
	

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(long totalFlow) {
		this.totalFlow = totalFlow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(totalFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.totalFlow = in.readLong();
	}

	@Override
	public String toString() {
		return + upFlow + "|" + downFlow;
	}
	
}
