package com.bit2017.mapreduce.io;

import java.io.*;

import org.apache.hadoop.io.*;

public class ItemFrequency implements Writable{
	
	private String item;
	private Long frequency;
	
	public ItemFrequency(Long frequency) {
		this.frequency = frequency;
	}
	
	public String getItem() {
		return item;
	}
	public void setItem(String item) {
		this.item = item;
	}
	public Long getFrequency() {
		return frequency;
	}
	public void setFrequency(Long frequency) {
		this.frequency = frequency;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		item = WritableUtils.readString(in);
		frequency = in.readLong();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, item);
		out.writeLong(frequency);
	}
	
	
	
	
}
