package ques1a;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NoOfPetitionOfDEnew {

	public static class PetitionMapper extends Mapper<LongWritable,Text,IntWritable, IntWritable>{
		IntWritable yearAsKey = new IntWritable();
		private final static IntWritable counter = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line= value.toString().split("\t");
			if(line[4].contains("DATA ENGINEER"))
			{
			int year= Integer.parseInt(line[7]);
			yearAsKey.set(year);
			context.write(yearAsKey,counter);
			}
		}
	}
	public static class PetitionReducer extends Reducer<IntWritable,IntWritable,Text,DoubleWritable>{
		DoubleWritable result =new DoubleWritable();
		int year=2011;						// for printing output format
		double eachYearVal[] = new double[6];
		int j=0;
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			double sum=0.0;
			double avggrowth=0.0;
			for(IntWritable val: values)
			{
				sum+=val.get();				//count of total number of jobtitle for particular year
			}
			eachYearVal[j]=sum;				// storing the count of each year in array
	
			if(eachYearVal[j]!= 0.0)        
			{
				if (j==0) 					//for first year value no average
				{
			        context.write(new Text(year +" "), new DoubleWritable(0));  // 2011	0
				}
				else 
				{
					avggrowth= ((eachYearVal[j]-eachYearVal[j-1])*100/eachYearVal[j-1]);
					result.set(Math.round(avggrowth));
					context.write( new Text(year +"-"+(year+1)), result);
					year++;
				}
			}
			j++;							// increment array to store value for next year
		}	
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration conf = new Configuration();
		Job job= Job.getInstance(conf,"de-petition");
		job.setJarByClass(NoOfPetitionOfDEnew.class);
		
		job.setMapperClass(PetitionMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(PetitionReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


