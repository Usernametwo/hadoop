package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class read {
	public static void main(String[] args) throws IOException {
		if(args.length < 1) {
			System.out.println("Usage read<Drc_path><Des_path>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream fsin = fs.open(s_path);
		byte[] buff = new byte[128];
		int length = 0;
		while((length = fsin.read(buff, 0, 128)) != -1){
			System.out.println(new String(buff, 0, length-1));
		}
		fsin.close();
		fs.close();
	}
}
