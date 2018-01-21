package ques1b;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Top5AvgGrowth  {

	public static class PetitionMapper extends Mapper<LongWritable,Text,Text,Text>{
		Text jobAsKey = new Text();
		Text yearAsValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line= value.toString().split("\t");
			String year= line[7];
			String job_title= line[4];
			jobAsKey.set(job_title);
			yearAsValue.set(year);
			context.write(jobAsKey,yearAsValue);
		}
	}
		
	public static class PetitionReducer extends Reducer<Text,Text,DoubleWritable,Text>{
		DoubleWritable result =new DoubleWritable();
		int year=2011;
		Text allValue=new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double yr11=0.0,yr12=0.0,yr13=0.0,yr14=0.0,yr15=0.0,yr16=0.0,finalgrowth=0.0;
			for(Text val:values)
			{					
				String year=val.toString();
				
				if(year.equals("2011"))
				{
					yr11++;
				}
				 else if(year.equals("2012"))
				{
					yr12++;
				}
				 else if(year.equals("2013"))
				{
					yr13++;
				}
				else if(year.equals("2014"))
				{
					yr14++;
				}
				 else if(year.equals("2015"))
				{
					yr15++;
				}
				 else if(year.equals("2016"))
				{
					yr16++;
				}				
			}
				if(yr11!=0.0 && yr12!=0.0 && yr13!=0.0 && yr14!=0.0 & yr15!=0.0 && yr16!=0.0)
				{
				finalgrowth=(double)(Math.round((Math.round((yr12-yr11)/yr11)*100)+
						(Math.round((yr13-yr12)/yr12)*100)+
						(Math.round((yr14-yr13)/yr13)*100)+
						(Math.round((yr15-yr14)/yr14)*100)+
						(Math.round((yr16-yr15)/yr15)*100)))/5;
				
				context.write(new DoubleWritable(finalgrowth), new Text(key));	
				}
		}
	}
	
	static class SortMapper extends Mapper<LongWritable,Text,DoubleWritable,Text>
	{
		DoubleWritable keySet =new DoubleWritable();
		Text valueSet =new Text();
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{ 
			String[] valueArr = value.toString().split("\t");
			keySet.set(Double.parseDouble(valueArr[0]));
			valueSet.set(valueArr[1]);
			context.write(keySet,valueSet);	
		}
	}
		
	public static class SortReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable>
	{
		int counter=1;
		public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			for(Text val : value)
			{
				if(counter<=5)
				{				
					context.write(val, key);
					counter++;
				}
			}
		}
	}	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		if (args.length != 2) {
			System.out
					.printf("Usage: StockPercentChangeDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job= Job.getInstance(conf,"top 5 avg growth");
		job.setJarByClass(Top5AvgGrowth.class);
		
		job.setMapperClass(PetitionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(PetitionReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setInputFormatClass(TextInputFormat.class);
		Path outputPath1 = new Path("FirstMapper");
		FileOutputFormat.setOutputPath(job, outputPath1);
		FileSystem.get(conf).delete(outputPath1, true);
		job.waitForCompletion(true);
	//---------------------------------------------------------------
		Job job2= Job.getInstance(conf,"top 5 avg growth job2");
		job2.setJarByClass(Top5AvgGrowth.class);
		
		job2.setMapperClass(SortMapper.class);
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setReducerClass(SortReducer.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[1]), true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

/*
 * output:
BUSINESS ANALYST 2	4100.0
SENIOR SYSTEMS ANALYST JC60	3550.0
PROGRAMMER/ DEVELOPER	3450.0
BUSINESS SYSTEMS ANALYST 2	3283.0
SOFTWARE DEVELOPER 2	2900.0
 * 
 */

