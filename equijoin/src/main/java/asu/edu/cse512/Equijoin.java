package asu.edu.cse512;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	
public class Equijoin 
{
	public static class JoinMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String trimmedValue = value.toString().replaceAll("\\s","");
			Long mapperkey = Long.parseLong(trimmedValue.toString().split(",")[1]);
			System.out.println(mapperkey);
			context.write(new LongWritable(mapperkey), new Text(trimmedValue));
		}
	}																																			

	public static class JoinReducer extends Reducer<LongWritable, Text, LongWritable, Text> 
	{
	   protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	   {
		   String joinedRecord = "";
		   for (Text value : values) 
			{
				joinedRecord += value.toString() + "-"; 
			}
		   
		   joinedRecord = joinedRecord.substring(0, joinedRecord.length()-1);
		   String[] records = joinedRecord.split("-");
		   
		   for(int i=0; i<records.length-1; i++)
		   {
			   for(int j=i+1; j<records.length; j++)
			   {
				   if(!records[i].split(",")[0].equals(records[j].split(",")[0]))
				   {
					   joinedRecord = records[i] + "," + records[j];
					   context.write(key, new Text(joinedRecord));
				   }
			   }
		   }
		}
	}

	public static void main(String[] args) throws Exception 
	{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "EquiJoin");
	    job.setJarByClass(Equijoin.class);
	    job.setMapperClass(JoinMapper.class);
	    job.setReducerClass(JoinReducer.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
}