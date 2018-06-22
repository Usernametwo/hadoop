package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class mkdirs {
	public static void main(String[] args) throws IOException {
		if(args.length < 1) {
			System.out.println("Usage mkdirs<Drc_path>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		fs.mkdirs(s_path);
	}
}
