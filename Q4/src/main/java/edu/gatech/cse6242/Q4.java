package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 {
	
  public static class DegreeMapper extends Mapper<Object, Text, Text, Text> {

	    private Text source = new Text();
	    private Text target = new Text();
	    private Text sourceValue = new Text();
	    private Text targetValue = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      
	    	String line = value.toString();
	    	String tokens [] = line.split("\t");
	    	if(tokens.length > 1) {
		    	source.set(tokens[0]);
		    	target.set(tokens[1]);
		    	sourceValue.set("01");
		    	targetValue.set("10");
		        context.write(source, sourceValue);
		        context.write(target, targetValue);
		    }
	    	
	    }
  }

  public static class DegreeReducer extends Reducer <Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int inDegree = 0;
      int outDegree = 0;
      for (Text val: values) { 
    	 String v = val.toString();
    	 inDegree += Character.getNumericValue(v.charAt(0));
    	 outDegree += Character.getNumericValue(v.charAt(1));
      }
      
      int out = inDegree - outDegree;
      context.write(key, new IntWritable(out));
     }
   }
	
  public static class DifferenceMapper extends Mapper<Object, Text, Text, IntWritable> {

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      
	    	String line = value.toString();
	    	String tokens [] = line.split("\t");
	    
	        context.write(new Text(tokens[1]), new IntWritable(1));
	    }
  }
  
  public static class DifferenceReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val: values) { 
	    	 sum += val.get();
	      }
	      context.write(key, new IntWritable(sum));
	     }
  }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q4");
    
    /* TODO: Needs to be implemented */
    job.setJarByClass(Q4.class);
    job.setMapperClass(DegreeMapper.class);
    job.setReducerClass(DegreeReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1] +  "/tmp"));
    job.waitForCompletion(true);
    
    Job job2 = Job.getInstance(conf, "Q4");
    
    /* TODO: Needs to be implemented */
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(DifferenceMapper.class);
    job2.setCombinerClass(DifferenceReducer.class);
    job2.setReducerClass(DifferenceReducer.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job2, new Path(args[1] + "/tmp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
