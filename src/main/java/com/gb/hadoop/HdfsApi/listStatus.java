package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class listStatus {
	public static void main(String[] args) throws IOException {
		if(args.length < 1) {
			System.out.println("Usage listStatus<Drc_path>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] files = fs.listStatus(s_path);
		for(FileStatus fst : files) {
			System.out.println(fst.toString());
		}
	}
}
