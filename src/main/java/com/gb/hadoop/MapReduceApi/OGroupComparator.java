package com.gb.hadoop.MapReduceApi;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OGroupComparator extends WritableComparator {
	
	public OGroupComparator() {
		// TODO Auto-generated constructor stub
		super(OrderBean.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		OrderBean ob1 = (OrderBean) a;
		OrderBean ob2 = (OrderBean) b;
		return ob1.getItemid().compareTo(ob2.getItemid());
	}
}
