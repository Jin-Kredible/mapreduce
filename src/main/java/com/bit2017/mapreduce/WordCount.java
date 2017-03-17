package com.bit2017.mapreduce;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class WordCount {
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "WordCount");
		
		
		// 1. Job instance 초기화 과정
		//job.setJarByClass(WordCount.class);
		
		//실행
		job.waitForCompletion(true);
		
		
	}
}
