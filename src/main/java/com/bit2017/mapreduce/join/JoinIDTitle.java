package com.bit2017.mapreduce.join;

import java.io.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.bit2017.mapreduce.wordcount.*;

public class JoinIDTitle {

	public static class TitleDocIdMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
			context.write(value, new Text(key + "\t" + 1));
			
		}

	}
	
	public static class DocIdCountMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			context.write(key, new Text(value + "\t" + 2));
		}

	}


	public static class JobIdTitleReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			int count = 0;
			Text k = new Text();
			Text v = new Text();
			
			for( Text value : values ) {
				String info = value.toString();
				String[] tokens = info.split( "\t" );
				
				if( "1".equals( tokens[1] ) ) {
					k.set( tokens[0] + "[" + key.toString() + "]" );
				} else if( "2".equals( tokens[1] ) ) {
					v.set( tokens[0] );
				}
				
				count++;
			}
			
			//출력
			if( count != 2 ) {
				return;
			}
			
			context.write( k,  v );
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Join ID & Title");
		// 1. Job instance 초기화 과정
		job.setJarByClass(JoinIDTitle.class);

		// 파라미터 저장
		final String TITLE_DOCID = args[0];
		final String DOCID_COUNT = args[1];
		final String OUTPUT_DIR = args[2];

		/* 입력 관련 */
		MultipleInputs.addInputPath(job, new Path(TITLE_DOCID), KeyValueTextInputFormat.class, TitleDocIdMapper.class );
		MultipleInputs.addInputPath(job, new Path(DOCID_COUNT), KeyValueTextInputFormat.class, DocIdCountMapper.class  );
		/* 출력 관련 */
		job.setReducerClass(JobIdTitleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

		// 10. 실행
		job.waitForCompletion(true);

	}
}