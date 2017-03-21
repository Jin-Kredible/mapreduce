package com.bit2017.mapreduce;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.bit2017.mapreduce.io.*;

public class WordCount {
	
	private static Log log = LogFactory.getLog(WordCount.class);
	
	public static class MyMapper extends Mapper<LongWritable, Text, StringWritable, Numberwritable> {
		
		private StringWritable word = new StringWritable();
		Numberwritable one = new Numberwritable(1L);

		@Override
		protected void setup(Mapper<LongWritable, Text, StringWritable, Numberwritable>.Context context)
				throws IOException, InterruptedException {
			log.info("------> setup() called");
		}



		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, StringWritable, Numberwritable>.Context context)
				throws IOException, InterruptedException {
			log.info("-------------> map() called");
			String line = value.toString();
			StringTokenizer tokenize = new StringTokenizer(line, "\r\n\t,|()<> ''.:");
			while(tokenize.hasMoreTokens()) {
			
				word.set(tokenize.nextToken().toLowerCase());			
				context.write(word, one);
			}

			
		}


		@Override
		protected void cleanup(Mapper<LongWritable, Text, StringWritable, Numberwritable>.Context context)
				throws IOException, InterruptedException {
			log.info("----------------> cleanup() called");
		}

		//run 은 보통 오버라이드를 하지 않음
		/*	@Override
				public void run(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
						throws IOException, InterruptedException {
					// TODO Auto-generated method stub
					super.run(context);
				}
		*/
		
	}
	
	public static class MyReducer extends Reducer<Text, Numberwritable, Text, Numberwritable> {
		
		private Numberwritable sumWritable = new Numberwritable();
		
		@Override
		protected void reduce(Text key, Iterable<Numberwritable> values,
				Reducer<Text, Numberwritable, Text, Numberwritable>.Context context) throws IOException, InterruptedException {
				
			long sum =0;
			for(Numberwritable value : values) {
				sum += value.get();
			}
			sumWritable.set(sum);
			context.write(key, sumWritable);
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "WordCount");
		
		
		// 1. Job instance 초기화 과정
		job.setJarByClass(WordCount.class);
		
		//2. 맵퍼 클래스 지정
		job.setMapperClass(MyMapper.class);
		
		//3. 리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);
		
		//4. 출력키 타입
		job.setMapOutputKeyClass(StringWritable.class);
		
		//5. 출력밸류 타입
		job.setMapOutputValueClass(Numberwritable.class);
		
		//6. 입력파일 포맷 지정(생략)
		job.setInputFormatClass(TextInputFormat.class);
		
		//7. 출력파일 포맷 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		//8.입력파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//9.출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//10. 실행
		job.waitForCompletion(true);
		
		
	}
}
