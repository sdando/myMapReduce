import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class MyConverter extends Configured implements Tool {

	public static class MapClass
	    extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key,Text value,Context context)
		    throws IOException,InterruptedException{
			String[] citation=value.toString().split("\t");
			String valueString;
			valueString=citation[0]+","+citation[1]+","+citation[2];			
			context.write(new Text(citation[0]), new Text(valueString));			
		}
	}
	
    public static class Reduce extends Reducer<Text, 
        Text, Text, Text>{
    	public void reduce(Text key,Iterable<Text> values,
    			Context context) throws IOException,InterruptedException{
    		String csv="";
    		for(Text val:values)
    			csv+=val.toString();
    		context.write(null, new Text(csv));
    	}
    }
    
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration configuration=getConf();
		Job job=new Job(configuration,"MyJob");
		job.setJarByClass(MyConverter.class);
		Path inPath=new Path(args[0]);
		Path outPath=new Path(args[1]);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new Configuration(), new MyConverter(),args));

	}

}
