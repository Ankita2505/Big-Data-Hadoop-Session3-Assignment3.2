package com.filterinvalidrecords;
/*
 * Map Reduce program to filter out invalid Records
 */

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FilterRecords {

	
	public static class FilterMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		//public static final Logger _logger = Logger.getAnonymousLogger();
		public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
			
			if(recordIsBad(value)){
				
				//Do Nothing not writting any loggers in this program
			}
			else
			{
				context.write(value, new IntWritable());
			}

			
			
			//The mark/value "9" is appended to every text of line.
						
		}
		
		private boolean recordIsBad(Text record)
		{
			boolean status = false;
			String line = record.toString();
			StringTokenizer tokens = new StringTokenizer(line,"|");
			
			while(tokens.hasMoreTokens()){
				if(tokens.nextToken().equalsIgnoreCase("NA")) {
					status = true;
				}
			}
			
			return status;
			
						
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MapJobOnly");
		
		SkipBadRecords skiprecords = new SkipBadRecords();
		skiprecords.setMapperMaxSkipRecords(conf, 50);
		/*
		 * hadoop jar command purpose to detect the main job class
		 */
		job.setJarByClass(FilterRecords.class);

		job.setMapOutputKeyClass(IntWritable.class);//key 2
		job.setMapOutputValueClass(Text.class);//value 2

		job.setOutputKeyClass(IntWritable.class);//final key
		job.setOutputValueClass(Text.class);//final value
		
		job.setNumReduceTasks(0);//setting number of reducers to 0

		job.setMapperClass(FilterMapper.class);//setting custom mapper class
		
		job.setInputFormatClass(TextInputFormat.class);//setting input format class
		job.setOutputFormatClass(TextOutputFormat.class);//setting output format class
Path OutputPath=new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));//input file or folder
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//output folder
		OutputPath.getFileSystem(conf).delete(OutputPath);

		boolean success = job.waitForCompletion(true);//executing the job
		System.exit(success ? 0 : 1);
	}

}
