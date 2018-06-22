package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class delete {
	public static void main(String[] args) throws IOException {
		if(args.length < 1) {
			System.out.println("Usage delete<Drc_path>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		boolean flag = fs.delete(s_path, false);
		if(flag == true) {
			System.out.println("success");
		} else {
			System.out.println("failed");
		}
	}
}
