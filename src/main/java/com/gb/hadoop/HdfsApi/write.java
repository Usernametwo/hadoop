package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class write {
	public static void main(String[] args) throws IOException {
		if(args.length < 1) {
			System.out.println("Usage write<Drc_path><Des_path>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FSDataOutputStream fsout = fs.append(s_path);
		byte[] buffer = "i am an student".getBytes();
		fsout.write(buffer);
		fsout.close();
		fs.close();
	}
}
