package com.bit2017.mapreduce.sort;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class StringSort {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "String sort");
		// 1. Job instance 초기화 과정
		job.setJarByClass(StringSort.class);
		
		//2. 맵퍼 클래스 지정
		job.setMapperClass(Mapper.class);
		
		//3. 리듀서 클래스 지정
		job.setReducerClass(Reducer.class);
		
		//4. 출력키 타입
		job.setMapOutputKeyClass(Text.class);
		
		//5. 출력밸류 타입
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);
		
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
