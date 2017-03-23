package com.bit2017.mapreduce.search;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.bit2017.mapreduce.io.*;
import com.bit2017.mapreduce.join.*;
import com.bit2017.mapreduce.join.JoinIDTitle.*;
import com.bit2017.mapreduce.topn.*;
import com.bit2017.mapreduce.wordcount.*;
import com.bit2017.mapreduce.wordcount.SearchText.*;

public class SearchDocs {

	Configuration conf = new Configuration();

	private static Log log = LogFactory.getLog(SearchText.class);

	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {

		private Text word = new Text();
		LongWritable one = new LongWritable(1L);
		

		@Override
		protected void setup(Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("------> setup() called");
		}

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			/* log.info("-------------> map() called"); */
			Configuration conf = context.getConfiguration();
			String search = conf.get("SearchText");
			String line = value.toString();

			log.info("line -----------------> " + line);
			log.info("search----------------->" + search);

			/*StringTokenizer tokenize = new StringTokenizer(line, "\r\n\t,|()<> ''.:");
			int count1 =0;
			while (tokenize.hasMoreTokens()) {
				String saveToken = tokenize.nextToken();
				log.info("----------->tokenize worked");
				if (saveToken.equals(search)) {
					log.info("came into for loops ---------->");
					word.set(saveToken.toLowerCase());
					count1+=1;
					context.write(key, new LongWritable(count1));
				}
			}*/
			
			int lastIndex = 0;
			int count = 0;

			while(lastIndex != -1){

			    lastIndex = line.indexOf(search,lastIndex);

			    if(lastIndex != -1){
			        count ++;
			        lastIndex += search.length();
			    }
			}
			log.info("key -----------> " + key + "....count -------------->" + count);
			context.write(key, new LongWritable(count));
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, Text> {

		private LongWritable sumWritable = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {

			long sum = 0;
			for(LongWritable value : values) {
				sum +=value.get();
			}
			sumWritable.set(sum);
			// context.getCounter("Words Status", "Count of all
			// Words").increment(sum);
			context.getCounter("Words Status", "Count unique words").increment(sum);
			

			context.write(key, new Text(sumWritable.toString()));

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		conf.set("SearchText", args[2].toString());

		Job job = new Job(conf, "Search Docs");

		log.info("args 2 ---------------------->" + args[2]);
		// 1. Job instance 초기화 과정
		job.setJarByClass(SearchDocs.class);

		// 2. 맵퍼 클래스 지정
		job.setMapperClass(MyMapper.class);

		// 3. 리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);

		// 4. 출력키 타입
		job.setMapOutputKeyClass(Text.class);

		// 5. 출력밸류 타입
		job.setMapOutputValueClass(LongWritable.class);

		// 6. 입력파일 포맷 지정(생략)
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 7. 출력파일 포맷 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);

		// 8.입력파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 9.출력 디렉토리 지정gg
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 10. 실행
		if (job.waitForCompletion(true) == false) {
			return;
		}

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Top N");

		job2.setJarByClass(TopN.class);
		
		job.getConfiguration().setInt("topN", Integer.parseInt(args[5]));

		job2.setMapperClass(TopN.MyMapper.class);

		// 3. 리듀서 클래스 지정
		job2.setReducerClass(TopN.MyReducer.class);

		// 4. 출력키 타입
		job2.setMapOutputKeyClass(Text.class);

		// 5. 출력밸류 타입
		job2.setMapOutputValueClass(LongWritable.class);

		// 6. 입력파일 포맷 지정(생략)
		job2.setInputFormatClass(KeyValueTextInputFormat.class);

		// 7. 출력파일 포맷 지정(생략 가능)
		job2.setOutputFormatClass(TextOutputFormat.class);

		// 8.입력파일 이름 지정
		FileInputFormat.addInputPath(job2, new Path(args[1]));

		// 9.출력 디렉토리 지정gg
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/topN"));
		
		

		if (job2.waitForCompletion(true) == false) {
			return;
		}
		
		
		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3, "Join ID & Title");
		// 1. Job instance 초기화 과정
		job3.setJarByClass(JoinIDTitle.class);

		// 파라미터 저장
		final String SEARCHDOC_TOPN = args[1] + "/topN";
		final String TITLE_ID = args[3];
		final String OUTPUT_DIR = args[4];

		/* 입력 관련 */
		MultipleInputs.addInputPath(job3, new Path(SEARCHDOC_TOPN), KeyValueTextInputFormat.class,
				JoinIDTitle.TitleDocIdMapper.class);
		MultipleInputs.addInputPath(job3, new Path(TITLE_ID), KeyValueTextInputFormat.class, JoinIDTitle.DocIdCountMapper.class);
		/* 출력 관련 */
		job3.setReducerClass(JobIdTitleReducer.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job3, new Path(OUTPUT_DIR));

		// 10. 실행
		job3.waitForCompletion(true);

	}
}
