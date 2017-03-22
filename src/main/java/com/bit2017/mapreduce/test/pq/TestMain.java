package com.bit2017.mapreduce.test.pq;

import java.util.*;

public class TestMain {

	public static void main(String[] args) {
		
		PriorityQueue<String> pq = new PriorityQueue<String>(10, new StringComparator());
		pq.add("hello");
		pq.add("ddddddddddd");
		pq.add("d");
		pq.add("232f23f23");
		pq.add("dddddddddddddddddddd");
		
		
		while(pq.isEmpty()==false) {
			String s= pq.remove();
			System.out.println(s);
		}	
	}
	
	
}
