package com.bit2017.mapreduce.index;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class InvertedIndex {
	public static class MyMapper extends Mapper<Text, Text, Text, Text> {

		private Text word = new Text();

		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/* log.info("-------------> map() called"); */
			String line = contents.toString();

			StringTokenizer tokenize = new StringTokenizer(line, "\r\n\t,|()<> ''.:");

			/* log.info("----------->tokenize worked"); */
			while (tokenize.hasMoreTokens()) {
				String token = tokenize.nextToken().toLowerCase();
				word.set(token);
				context.write(word, docId);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text word, Iterable<Text> docIds,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
/*			long unique = 0;*/
			StringBuilder sb =new StringBuilder();
			
			boolean isFirst = true;
			for (Text docId : docIds) {
				if(isFirst==false) {
					sb.append(", ");
				} else {
					isFirst = false;
				}
				sb.append(docId.toString());
			}

			context.write(word, new Text(sb.toString()));

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Inverted Index");
		// 1. Job instance 초기화 과정
		job.setJarByClass(InvertedIndex.class);

		// 2. 맵퍼 클래스 지정
		job.setMapperClass(MyMapper.class);

		// 3. 리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);

		// 4. 맵출력키 타입
		job.setMapOutputKeyClass(Text.class);

		// 5. 맵 출력밸류 타입
		job.setMapOutputValueClass(Text.class);

		// 4. 리듀스 출력키 타입
		job.setOutputKeyClass(Text.class);

		// 5. 리듀스 출력밸류 타입
		job.setOutputValueClass(Text.class);

		// 6. 입력파일 포맷 지정(생략)
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 7. 출력파일 포맷 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);

		// 8.입력파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 9.출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 10. 실행
		job.waitForCompletion(true);

	}
}
