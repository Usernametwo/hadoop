package com.gb.hadoop.MapReduceApi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OrderBean  implements WritableComparable<OrderBean>{

	private Text itemid;
	private DoubleWritable amount;
	public OrderBean() {
		// TODO Auto-generated constructor stub
	}
	
	public OrderBean(Text itemid, DoubleWritable amount) {
		this.itemid = itemid;
		this.amount = amount;
	}
	
	

	public Text getItemid() {
		return itemid;
	}

	public void setItemid(Text itemid) {
		this.itemid = itemid;
	}

	public DoubleWritable getAmount() {
		return amount;
	}

	public void setAmount(DoubleWritable amount) {
		this.amount = amount;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.itemid.toString());
		out.writeDouble(this.amount.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.itemid = new Text(in.readUTF());
		this.amount = new DoubleWritable(in.readDouble());
	}

	@Override
	public int compareTo(OrderBean o) {
		// TODO Auto-generated method stub
		int ret = this.itemid.compareTo(o.getItemid());
		if(ret == 0) {
			ret = -this.amount.compareTo(o.getAmount());
		}
		return ret;
	}

	@Override
	public String toString() {
		return "OrderBean [itemid=" + itemid + ", amount=" + amount + "]";
	}
	
}
