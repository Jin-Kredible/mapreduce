package com.bit2017.mapreduce.topn;
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

public class TopN {

		
		public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		
			private int topN = 10;
			private PriorityQueue<ItemFreq> pq = null;
			
			@Override
			protected void setup(Mapper<Text, Text, Text, LongWritable>.Context context)
					throws IOException, InterruptedException {
				topN = context.getConfiguration().getInt("topN", 10);
				pq = new PriorityQueue<ItemFreq>(10, new ItemFreqComparator());
				
			}
			
			
			@Override
			protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
					throws IOException, InterruptedException {
				ItemFreq newItemFreq = new ItemFreq();
				newItemFreq.setItem(key.toString());
				newItemFreq.setFreq(Long.parseLong(value.toString()));
				
				ItemFreq head = pq.peek();
				if( pq.size() <topN|| head.getFreq()< newItemFreq.getFreq()) {
					pq.add(newItemFreq);
				}
				
				if(pq.size()>topN) {
					pq.remove();
				}
			}
			
			@Override
			protected void cleanup(Mapper<Text, Text, Text, LongWritable>.Context context)
					throws IOException, InterruptedException {
				while(pq.isEmpty()==false) {
					ItemFreq itemFreq = pq.remove();
					context.write(new Text(itemFreq.getItem()), new LongWritable(itemFreq.getFreq()));
				}
			}
			
		}
		
		public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
			private int topN = 10;
			private PriorityQueue<ItemFreq> pq = null;
			
			@Override
			protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
					throws IOException, InterruptedException {
				topN = context.getConfiguration().getInt("topN", 10);
				pq = new PriorityQueue<ItemFreq>(10, new ItemFreqComparator());
			}
			
			
			@Override
			protected void reduce(Text key, Iterable<LongWritable> value,
					Reducer<Text, LongWritable, Text, LongWritable>.Context arg2)
					throws IOException, InterruptedException {
				Long sum =0L;
				for(LongWritable values : value) {
					sum += values.get();
				}
				
				ItemFreq newItemFreq = new ItemFreq();
				newItemFreq.setItem(key.toString());
				newItemFreq.setFreq(sum);
				
				ItemFreq head = pq.peek();
				if(pq.size() <topN || head.getFreq()< newItemFreq.getFreq()) {
					pq.add(newItemFreq);
				}
				
				if(pq.size()>topN) {
					pq.remove();
				}
			}
			
			@Override
			protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
					throws IOException, InterruptedException {
				while(pq.isEmpty()==false) {
					ItemFreq itemFreq = pq.remove();
					context.write(new Text(itemFreq.getItem()), new LongWritable(itemFreq.getFreq()));
				}
			}
			
			
		}

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "TopN");
			// 1. Job instance 초기화 과정
			job.setJarByClass(TopN.class);
			
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
			job.setOutputFormatClass(TextOutputFormat.class);
			
			
			//8.입력파일 이름 지정
			FileInputFormat.addInputPath(job, new Path(args[0]));
			
			//9.출력 디렉토리 지정gg
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//10. N 파라미터
			job.getConfiguration().setInt("topN", Integer.parseInt(args[2]));
			
			//11. 실행
			job.waitForCompletion(true);
			
			
		}
	}