package com.acadgild.assigment2;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvalidTVMapper extends Mapper <LongWritable, Text, Text, IntWritable> {

	IntWritable  count;
	Text state;
	@Override
	public void setup(Context context) {
		count = new IntWritable(1);
		state = new Text();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		Predicate<String> predicate=p-> p.equalsIgnoreCase("Onida");
		List<String> onidaSales=Arrays.stream(value.toString().split("\\|")).filter(predicate).collect(toList());
		state.set(onidaSales.get(4));
		context.write(state, count);
	}
}
