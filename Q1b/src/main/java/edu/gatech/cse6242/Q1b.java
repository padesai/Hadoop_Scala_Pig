package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Q1b {

	public static class TargetWeightSource implements WritableComparable<TargetWeightSource> {
		private String source;
		private String weight;
		private String target;
		
		public void setTarget(String t) {
			target = t;
		}
		
		public void setWeight(String w) {
			weight = w;
		}
		
		public void setSource(String s) {
			source = s;
		}
		
		public String getTarget() {
			return this.target;
		}
		
		public String getWeight() {
			return this.weight;
		}
		
		public String getSource() {
			return this.source;
		}
		
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, target);
			WritableUtils.writeString(out, weight);
			WritableUtils.writeString(out, source);
			
		}
		
		public void readFields(DataInput in) throws IOException {
			this.target = WritableUtils.readString(in);
			this.weight = WritableUtils.readString(in);
			this.source = WritableUtils.readString(in);
			
		}
		
		
		@Override 
			public int compareTo(TargetWeightSource targetWeightSource){
				int compareValue = this.target.compareTo(targetWeightSource.getTarget());
				if (compareValue == 0 ) {
					compareValue = -1*this.weight.compareTo(targetWeightSource.getWeight());
					if (compareValue == 0) {
						compareValue  = this.source.compareTo(targetWeightSource.getSource());
					}
				}
				return compareValue;
			}
		@Override
			public String toString() {
				return target.toString() + ":" + weight.toString() + ":" + source.toString();
			}
	}	
	
  public static class SourceNodeWeightMapper extends Mapper<Object, Text, TargetWeightSource, IntWritable> {
	    
	    TargetWeightSource targetWeightSource = new TargetWeightSource();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      
	    	String line = value.toString();
	    	String tokens [] = line.split("\t");
	        targetWeightSource.setTarget(tokens[1]);
	        targetWeightSource.setWeight(tokens[2]);
	        targetWeightSource.setSource(tokens[0]);
	        context.write(targetWeightSource, new IntWritable(Integer.parseInt(tokens[0])));
	    }
  }

  public static class MaxWeightReducer extends Reducer <TargetWeightSource, IntWritable, IntWritable, IntWritable> {

    public void reduce(TargetWeightSource key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	Iterator<IntWritable> iter = values.iterator();
    	IntWritable target = new IntWritable(Integer.parseInt(key.getTarget()));
    	context.write(target,iter.next());
   }    
 }

  public static class TargetGroupingComparator extends WritableComparator {
  	public TargetGroupingComparator() {
  		super(TargetWeightSource.class, true);
  	}
  	
  	@Override
  	public int compare(WritableComparable tp1, WritableComparable tp2) {
  		TargetWeightSource targetWeightSource = (TargetWeightSource) tp1;
  		TargetWeightSource targetWeightSource2 = (TargetWeightSource) tp2;
  		return targetWeightSource.getTarget().compareTo(targetWeightSource2.getTarget());
  	}
  }
  
  public static class TargetPartitioner extends Partitioner<TargetWeightSource, NullWritable> {
  	@Override
  	public int getPartition(TargetWeightSource targetWeightSource, NullWritable nullWritable, int numPartitions) {
  		return targetWeightSource.getTarget().hashCode() % numPartitions;
  	}
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1b");

    /* TO DO: Needs to be implemented */
    job.setJarByClass(Q1b.class);
    job.setMapperClass(SourceNodeWeightMapper.class);
    job.setReducerClass(MaxWeightReducer.class);
    job.setPartitionerClass(TargetPartitioner.class);
    job.setGroupingComparatorClass(TargetGroupingComparator.class);
    job.setMapOutputKeyClass(TargetWeightSource.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
