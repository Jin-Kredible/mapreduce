package com.bit2017.mapreduce.io;

import java.io.*;

import org.apache.hadoop.io.*;

public class Numberwritable implements Writable {

	
	private Long number;
	
	public Numberwritable() {
		
	}
	
	public Numberwritable(Long number) {
		this.number = number;
	}
	
	
	public Long get() {
		return number;
	}
	
	public void set(Long sum) {
		this.number = number;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		number = WritableUtils.readVLong(in);
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		WritableUtils.writeVLong(out, number);
	}

}
