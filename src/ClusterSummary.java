import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.Vector;

public class ClusterSummary {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Path examplarPath=new Path(args[0]);
		Path sarPath=new Path(args[1]);
		Path outPath=new Path(args[2]);
		
		Configuration conf=new Configuration();
		conf.set("examplar", examplarPath.toString());
		Job job=new Job(conf,"Cluster Summary");
		job.setJarByClass(ClusterSummary.class);
		job.setMapperClass(DisMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, sarPath);
		FileOutputFormat.setOutputPath(job, outPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class DisMapper extends Mapper<Text,Point, IntWritable, Text>{
		
		Vector examplarVector;
		protected void setup(Context ctx) throws IOException, InterruptedException {
			String examplar=ctx.getConfiguration().get("examplar");
			examplarVector=Vectors.readSequenceFile(new Path(examplar), ctx.getConfiguration());   			   
		}
		
	    protected void map(Text key, Point pt, Context ctx)
	            throws IOException, InterruptedException {
	    	}
	    }
	
	public static class SumReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable>{
	    
		protected void reduce(IntWritable key, Iterable<Text> values, Context ctx)
	            throws IOException, InterruptedException {
	    	
	    }
		
	    protected void cleanup(Context ctx) throws IOException, InterruptedException {
	        super.cleanup(ctx);
	        
	      }
	}
}