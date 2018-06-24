package com.gb.hadoop.MapReduceApi;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OgroupComparator extends WritableComparator {
	
	public OgroupComparator() {
		// TODO Auto-generated constructor stub
		super(OrderBean.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		return super.compare(a, b);
	}
}
