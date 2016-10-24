package com.acadgild.assigment2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvalidTVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable sum;
	
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		sum=new IntWritable();
	}
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,Context context) throws IOException, InterruptedException {
		int count = Integer.MIN_VALUE;
		for(IntWritable lineCount:arg1){
			count+=lineCount.get();
		}
		sum.set(count);
		context.write(arg0, sum);
	}

}
