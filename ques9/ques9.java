package ques9;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class EmpSuccessRate {

	public static class EmpMapper extends Mapper<LongWritable,Text,Text,Text>{
		private final static IntWritable count=new IntWritable(1);	
		Text mapKey=new Text();
		Text mapValue=new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line=value.toString().split("\t");
			
			String caseStatus=line[1];
			String empName=line[2];
			
			mapKey.set(empName);
			mapValue.set(caseStatus+" , "+count);
			context.write(mapKey,mapValue);				//key : empname value : case_status +1
			
		}
	}
	public static class EmpReducer extends Reducer<Text,Text,Text,Text>{
		Text redValue=new Text();
		
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			double sum=0.0,certified=0.0,withdrawn=0.0,successrate=0.0, finalsum=0.0;
			for(Text val: values)
			{
			String[] reducerline= val.toString().split(",");
			Double countValue=Double.parseDouble(reducerline[1]);
			String caseStatus=reducerline[0];
			sum=sum+countValue;				//total record
		
				if(caseStatus.equals("CERTIFIED "))
				{
					certified++;	
				}
				else if(caseStatus.equalsIgnoreCase("CERTIFIED-WITHDRAWN "))
				{
					withdrawn++;
				}
				if(sum>=1000)
				{
				successrate=((certified + withdrawn)/sum)*100;
				}
			}
			
			if(successrate>70.0)
			{
			redValue.set(sum+" , "+Math.round(successrate)+"%");	
			context.write(key,redValue);	
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf= new Configuration();
		Job job=Job.getInstance(conf,"emp-success-rate");
		job.setJarByClass(EmpSuccessRate.class);
		
		job.setMapperClass(EmpMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(EmpReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

