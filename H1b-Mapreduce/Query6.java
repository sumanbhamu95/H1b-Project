import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Query6 {
	public static class ma extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split("\t");
	
			context.write(new Text(arr[7]), new Text(arr[1]));
		
	}}
public static class re extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		int c=0;
		int count=0;int w=0;int  d=0,cw=0;String str="";
		for(Text a:value){
		count++;
		if(a.toString().equals("CERTIFIED")){
			
		c++;
			
		}
		else if(a.toString().equals("CERTIFIED-WITHDRAWN")){
			cw++;
		}
		else if(a.toString().equals("WITHDRAWN")){
			w++;
		}
		else if(a.toString().equals("DENIED")){
			d++;
		}
		}
		str=count+","+c+","+cw+","+w+","+d+","+((double)(c*100)/count)+","+((double)(cw*100)/count)+","+((double)(w*100)/count)+","+((double)(d*100)/count);
		context.write(key, new Text(str));
	}
	
}
public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException{
	Configuration c=new Configuration();
	Job job=Job.getInstance(c,"sss");
	job.setJarByClass(Query6.class);
	job.setMapperClass(ma.class);
	job.setReducerClass(re.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	FileSystem.get(c).delete(new Path(args[1]),true);
	FileInputFormat.addInputPath(job, new Path(args[0]));	
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)?0:1);

}

}