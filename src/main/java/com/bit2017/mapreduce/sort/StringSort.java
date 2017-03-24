package com.bit2017.mapreduce.sort;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class StringSort {
	
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		
		private Text word = new Text();
		LongWritable one = new LongWritable(1L);


		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			/*log.info("-------------> map() called");*/
			String line = value.toString();
			
			StringTokenizer tokenize = new StringTokenizer(line, "\r\n\t,|()<> ''.:");
			
			
			/*	log.info("----------->tokenize worked");*/
			while(tokenize.hasMoreTokens()) {
				word.set(tokenize.nextToken().toLowerCase());
				context.write(word, one);
			}
	
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		private LongWritable sumWritable = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

			long unique = 0;
			for(LongWritable value : values) {
				unique +=value.get();
			}
			
			sumWritable.set(unique);
			context.getCounter("Words Status", "Count unique words").increment(unique);
			
			context.write(key, sumWritable);
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "String sort");
		// 1. Job instance 초기화 과정
		job.setJarByClass(StringSort.class);
		
		//2. 맵퍼 클래스 지정
		job.setMapperClass(MyMapper.class);
		
		//3. 리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);
		
		//4. 출력키 타입
		job.setMapOutputKeyClass(Text.class);
		
		//5. 출력밸류 타입
		job.setMapOutputValueClass(LongWritable.class);
		
		//6. 입력파일 포맷 지정(생략)
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//7. 출력파일 포맷 지정(생략 가능)
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		
		//8.입력파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//9.출력 디렉토리 지정gg
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		
		
		//10. 실행
		job.waitForCompletion(true);
		
		
	}
}
