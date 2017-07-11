/*import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class query4 {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split("\t");

			context.write(new Text(arr[2]), new Text(arr[7]+" "+arr[1]));
		}
	}	
	public static class Mypartitioner extends Partitioner<Text, Text>
	{	
		public int getPartition(Text key, Text value, int arg2) {
	        String s[]=value.toString().split(" ");
	        int i=Integer.parseInt(s[0]);
	        if(i==2011)
	        {
	        	return 0;
	        }
	        if(i==2012)
	        {
	        	return 1;
	        }
	        if(i==2013)
	        {
	        	return 2;
	        }
	        if(i==2014)
	        {
	        	return 3;
	        }
	        if(i==2015)
	        {
	        	return 4;
	        }
			if(i==2016)
			{
				return 5;
			}
			return i;
		}
		
	}
		public static class Myreducer extends Reducer<Text,Text,NullWritable,Text>
		{
			TreeMap tm1=new TreeMap();
			TreeMap tm2=new TreeMap();
			TreeMap tm3=new TreeMap();
			TreeMap tm4=new TreeMap();
			TreeMap tm5=new TreeMap();
			TreeMap tm6=new TreeMap();
			public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			String k=key.toString();
			String kk="";
				int count=0,year=0;
				
				for(Text a:value){
					String ss[]=a.toString().split(" ");
				count++;	
				year=Integer.parseInt(ss[0]);
				kk=k+"  "+count;
			}
				if(year==2011)
				{
				tm1.put(count,new Text(kk));
				if(tm1.size()>5)
				{
					tm1.remove(tm1.firstKey());
				}}
				if(year==2012)
				{
				tm2.put(count,new Text(kk));
				if(tm2.size()>5)
				{
					tm2.remove(tm2.firstKey());
				}}
				if(year==2013)
				{
				tm3.put(count,new Text(kk));
				if(tm3.size()>5)
				{
					tm3.remove(tm3.firstKey());
				}}
				if(year==2014)
				{
				tm4.put(count,new Text(kk));
				if(tm4.size()>5)
				{
					tm4.remove(tm4.firstKey());
				}}
				if(year==2015)
				{
				tm5.put(count,new Text(kk));
				if(tm5.size()>5)
				{
					tm5.remove(tm5.firstKey());
				}}
				if(year==2016)
				{
				tm6.put(count,new Text(kk));
				if(tm6.size()>5)
				{
					tm6.remove(tm6.firstKey());
				}}
				
			}
			public void cleanup(Context context) throws IOException, InterruptedException
			{
				for(Text b:tm1.descendingMap().values())
				{
					context.write(NullWritable.get(),b);
				}
				for(Text b:tm2.descendingMap().values())
				{
					context.write(NullWritable.get(),b);
				}
				for(Text b:tm3.descendingMap().values())
				{
					context.write(NullWritable.get(),b);
				}
				for(Text b:tm4.descendingMap().values())
				{
					context.write(NullWritable.get(),b);
				}
				for(Text b:tm5.descendingMap().values())
				{
					context.write(NullWritable.get(),b);
				}
				for(Text b:tm6.descendingMap().values())
				{
					context.write(NullWritable.get(),b);
				}
			}}
		public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException{
			Configuration conf=new Configuration();
			Job job=Job.getInstance(conf,"work");
			job.setJarByClass(query4.class);
			
			job.setMapperClass(Mymapper.class);
			job.setReducerClass(Myreducer.class);
			job.setPartitionerClass(Mypartitioner.class);
			job.setNumReduceTasks(6);
			//job.setCombinerClass(Myreducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileSystem.get(conf).delete(new Path(args[1]),true);
			FileInputFormat.addInputPath(job, new Path(args[0]));	
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			
			System.exit(job.waitForCompletion(true)?0:1);

		}	
}


*/