package com._2dmirum_mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver_MIRUM extends Configured implements Tool{

	@Override
	public int run(String[] arg) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		
//		FileInputFormat.setInputPaths(job, new Path("/home/akshay/Desktop/vectorDump.txt")); 
//		FileOutputFormat.setOutputPath(job, new Path("/home/akshay/Desktop/vectorDumpOutput"));
		
		FileInputFormat.setInputPaths(job, new Path(arg[0])); 
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		
		job.setMapperClass(Mapper_MIRUM.class);
		job.setReducerClass(Reducer_MIRUM.class);
		// job.setNumReduceTasks(3);
		// job.setCombinerClass(MyReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Driver_MIRUM(), args);
		System.out.println(exitCode);
	}
}
