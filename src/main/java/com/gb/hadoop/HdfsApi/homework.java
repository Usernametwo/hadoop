package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class homework {
	public static void main(String[] args) throws IOException{
		if(args.length < 2) {
			System.out.println("Usage homework<Dir_path><Upload_file>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Path upload_file = new Path(args[1]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		boolean flag = fs.mkdirs(s_path);
		if(flag) {
			System.out.println("mkdir success!");
		} else {
			System.out.println("mkdir failed");
		}
		fs.copyFromLocalFile(upload_file, s_path);
		
		String[] split_result = args[1].split("/");
		String path = args[0] + "/" + split_result[split_result.length - 1];
		System.out.println("upload path:" + path);
		Path hadoop_file = new Path(path);
		FSDataInputStream fsin = fs.open(hadoop_file);
		byte[] buffer = new byte[128];
		int length = 0;
		while((length = fsin.read(buffer, 0, 128)) != -1) {
			System.out.println(new String(buffer, 0, length-1));
		}
		fsin.close();
		fs.delete(hadoop_file, false);
		fs.close();
	}
}
