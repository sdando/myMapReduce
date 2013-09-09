import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class CountRef2 extends Configured implements Tool {
	public static class MapClass extends MapReduceBase 
	  implements Mapper<Text, Text, IntWritable, IntWritable>{
		private static final IntWritable uno=new IntWritable(1);
		private IntWritable citationCount=new IntWritable();
		public void map(Text key,Text value,
				OutputCollector<IntWritable,IntWritable> output,
				Reporter reporter) throws IOException {
			    citationCount.set(Integer.parseInt(value.toString()));
		        output.collect(citationCount,uno);
		}
	}
	
	public static class Reduce extends MapReduceBase
	  implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		public void reduce(IntWritable key,Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			int count=0;
			while(values.hasNext()){
				values.next();
				count++;
			}
			output.collect(key, new IntWritable(count));
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		JobConf jobConf=new JobConf(conf,CountRef2.class);
		
		Path in=new Path(args[0]);
		Path out=new Path(args[1]);
		FileInputFormat.setInputPaths(jobConf, in);
		FileOutputFormat.setOutputPath(jobConf, out);
		
		jobConf.setJobName("CountReference2");
		jobConf.setMapperClass(MapClass.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(IntWritable.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(jobConf);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res=ToolRunner.run(new Configuration(), new CountRef2(), args);
		System.exit(res);
	}

}
