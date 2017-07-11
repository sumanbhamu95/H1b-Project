import java.io.IOException;
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






public class Que5bt {
	public static class ma extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split("\t");
		if(arr[1].toString().equals("CERTIFIED")){
			
		
			context.write(new Text(arr[4]),value);
		}
	}}

	public static class mypart extends Partitioner<Text,Text>
	{
		public int getPartition(Text key,Text value,int numReduceTasks)
		{
			String str[] = value.toString().split("\t");
			if(numReduceTasks == 0)
			{
				return 0;
			}
			if(str[7].equals("2011"))
			{
				return 0;
			}
			if(str[7].equals("2012"))
			{
				return 1;
			}
			if(str[7].equals("2013"))
			{
				return 2;
			}
			if(str[7].equals("2014"))
			{
				return 3;
			}
			if(str[7].equals("2015"))
			{
				return 4;
			}
			if(str[7].equals("2016"))
			{
				return 5;
			}else
				return 6;
		}
	}
	public static class Myreducer extends Reducer<Text,Text,NullWritable,Text>
	{
		private TreeMap<Integer,Text>  hs = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  hs1 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  hs2 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  hs3 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  hs4 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  hs5 = new TreeMap<Integer,Text>();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int count=0;
			String year ="";
			String myval = key.toString();
			for(Text val :values)
			{
				String ss[] = val.toString().split("\t");
				count++;
				 year = ss[7];
			}
			 myval = myval+','+ year+','+count;
			if(year.equals("2011"))                        //year 2011 details
			{
				hs.put(count,new Text(myval));
				if(hs.size()>10)
				{
					hs.remove(hs.firstKey());
				}
			}
			if(year.equals("2012"))                         // year 2012 details
			{
				hs1.put(count,new Text(myval));
				if(hs1.size()>10)
				{
					hs1.remove(hs1.firstKey());
				}
			}
			if(year.equals("2013"))                           // year 2013 details
			{
				hs2.put(count,new Text(myval));
				if(hs2.size()>10)
				{
					hs2.remove(hs2.firstKey());
				}
			}
			if(year.equals("2014"))                           //year 2014 details
			{
				hs3.put(count, new Text(myval));
				if(hs3.size()>10)
				{
					hs3.remove(hs3.firstKey());
				}
			}
			if(year.equals("2015"))                             //year 2015 details
			{
				hs4.put(count,new Text(myval));
				if(hs4.size()>10)
				{
					hs4.remove(hs4.firstKey());
				}
			}
			if(year.equals("2016"))                               //year 2016 details
			{
				hs5.put(count,new Text(myval));
				if(hs5.size()>10)
				{
					hs5.remove(hs5.firstKey());
				}
			}}
			
			public void cleanup(Context context) throws IOException,InterruptedException
			{
				for(Text val:hs.descendingMap().values())
				{
					context.write(NullWritable.get(), val);
				}
				for(Text bl:hs1.descendingMap().values())
				{
					context.write(NullWritable.get(),bl);
				}
				for(Text bl:hs2.descendingMap().values())
				{
					context.write(NullWritable.get(),bl);
				}for(Text bl:hs3.descendingMap().values())
				{
					context.write(NullWritable.get(),bl);
				}for(Text bl:hs4.descendingMap().values())
				{
					context.write(NullWritable.get(),bl);
				}for(Text bl:hs5.descendingMap().values())
				{
					context.write(NullWritable.get(),bl);
				}
			}
		}
	public  static void main(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job name");
		job.setJarByClass(Que5bt.class);
		job.setPartitionerClass(mypart.class);
		job.setMapperClass(ma.class);
		job.setReducerClass(Myreducer.class);
		job.setNumReduceTasks(7);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}}