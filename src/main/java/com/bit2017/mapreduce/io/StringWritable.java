package com.bit2017.mapreduce.io;

import java.io.*;

import org.apache.hadoop.io.*;

public class StringWritable implements WritableComparable<StringWritable> {

	
	private String value;
	
	
	public void set(String value) {
		this.value = value;
	}
	
	public String get() {
		return this.value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		value = WritableUtils.readString(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, value);
	}

	@Override
	public int compareTo(StringWritable arg0) {
		return value.compareTo(arg0.get());
	}

	@Override
	public String toString() {
		return value;
	}
	
	

}
