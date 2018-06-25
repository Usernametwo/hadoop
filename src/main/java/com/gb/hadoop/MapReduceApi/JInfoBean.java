package com.gb.hadoop.MapReduceApi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JInfoBean implements Writable{
	
	private String orderId;
	private String pId;
	private String name;
	private String flag;
	
	public JInfoBean() {
		// TODO Auto-generated constructor stub
		orderId = "";
		pId = "";
		name = "";
		flag = "";
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getpId() {
		return pId;
	}

	public void setpId(String pId) {
		this.pId = pId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(orderId);
		out.writeUTF(pId);;
		out.writeUTF(name);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		orderId = in.readUTF();
		pId = in.readUTF();
		name = in.readUTF();
		flag = in.readUTF();
	}

	@Override
	public String toString() {
		return orderId + " " + pId + " " + name;
	}
}
