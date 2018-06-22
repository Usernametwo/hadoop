package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class rename {
	public static void main(String[] args) throws IOException {
		if(args.length < 2) {
			System.out.println("Usage rename<Drc_path><Des_path>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Path d_path = new Path(args[1]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		fs.rename(s_path, d_path);
		fs.close();
	}
}
