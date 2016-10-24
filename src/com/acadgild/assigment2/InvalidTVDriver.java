package com.acadgild.assigment2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map Reduce program to calculate the total units sold for each Company
 * @author Akhilesh.Agarwal
 * 
 *
 */
public class InvalidTVDriver {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
	    Job job = Job.getInstance(new Configuration(), "Invalid Tv Finder");

	    job.setJarByClass(InvalidTVDriver.class);

	    job.setMapperClass(InvalidTVMapper.class);
	    
	    job.setReducerClass(InvalidTVReducer.class);
	    
	    job.setOutputKeyClass(Text.class);

	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));

	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);

	}

}
