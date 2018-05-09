package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1a {

  public static class SourceNodeWeightMapper extends Mapper<Object , Text, Text, IntWritable> {

    private Text sourceNode = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
    	String line = value.toString();
    	String tokens [] = line.split("\t");
        sourceNode.set(tokens[0]);
        context.write(sourceNode, new IntWritable(Integer.parseInt(tokens[2])));
      }
    }

  public static class MaxWeightReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int max = 0;
      int iteration = 0;
      for (IntWritable val: values) {
    	
    	if (iteration == 0) {
    		max = val.get();
    	}
        if (val.get() > max) {
          max = val.get();
        }
        
        iteration++;
       
      }
      context.write(key, new IntWritable(max));
     }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1a");

    /* TO DO: Needs to be implemented */
    job.setJarByClass(Q1a.class);
    job.setMapperClass(SourceNodeWeightMapper.class);
    job.setCombinerClass(MaxWeightReducer.class);
    job.setReducerClass(MaxWeightReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
