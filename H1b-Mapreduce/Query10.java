


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




//10) Which are the  job positions along with the number of petitions which have the success rate more than 70% 
//in petitions (total petitions filed more than 1000)?


public class Query10 {
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String parts[] = value.toString().split("\t");
			context.write((new Text(parts[4])),value);//job_title
		}
	}
	public static class MyReducer extends Reducer<Text,Text,Text,FloatWritable>
	{
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int count = 0,count1 = 0,count2 = 0;
			double success;
			long myval = 0 ;
			for(Text t:values)
			{
				String parts[] = t.toString().split("\t");
				count++;
				if(parts[1].equals("CERTIFIED"))
				{
					count1++;
				}
				if(parts[1].equals("CERTIFIED-WITHDRAWN"))
				{
					count2++;
				}
			}
			//myval = key.toString();
			
			if(count > 1000)
			{
				success = (count1+count2)*100/count;
				if(success > 70.0)
				{
				myval = (long) success;
				}
			context.write(key,new FloatWritable(myval));
			}
		}
		
	}
	public static void main(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"aaaa");
		job.setJarByClass(Query10.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}