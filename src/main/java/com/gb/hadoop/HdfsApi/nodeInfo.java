package com.gb.hadoop.HdfsApi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class nodeInfo {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		DatanodeInfo[] dataNodeStats = null;
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		dataNodeStats = hdfs.getDataNodeStats();
		for(DatanodeInfo dni:dataNodeStats) {
			System.out.println(dni.toString());
		}
		fs.close();
	}
}
