package com.acadgild.assigment1;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvalidTVMapper extends Mapper <LongWritable, Text, Text, IntWritable> {

	IntWritable  count;
	Text company;
	@Override
	public void setup(Context context) {
		count = new IntWritable();
		company = new Text();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		List<String> companyName=Arrays.stream(value.toString().split("\\|")).limit(1).collect(toList());
		count.set(1);
		company.set(companyName.get(0));
		context.write(company, count);
	}
}
