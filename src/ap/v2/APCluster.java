package ap.v2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


public class APCluster extends Configured{
	
	private static final String sarTable="SAR";
	private static final String ExemplarTable="result";
	private static final String DIM_STRING="DIM";
	private static final String SIM_STRING="Sim";
	private static final String Avail_STRING1="Avail1";
	private static final String Avail_STRING2="Avail2";   //转置
	private static final String Res_STRING1="Res1";
	private static final String Res_STRING2="Res2";   //转置
	private static final String Exemplar_STRING="Exm";
	private static final String Old_Center="old";
	private static final String New_Center="new";
	
    public static class SMapper extends MapReduceBase implements
        Mapper<Text, Text, Text, Text>{
    	private int recordNum;
    	public void configure(JobConf conf){
    		recordNum=Integer.parseInt(conf.get("record.num"));
    	}
    	
    	public void map(Text key, Text value,OutputCollector<Text, Text> output,
    	    Reporter reporter) throws IOException{
    		int id=Integer.parseInt(key.toString());
    		for(int i=1;i<id;i++){
    			output.collect(new Text(i+" "+id), value); 
    		}
    		for(int j=id;j<=recordNum;j++){
    			output.collect(new Text(id+" "+j), value);
    		}
    	}
    }
    
    public static class SReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text>{
    	private String prefer;
    	public void configure(JobConf conf){
    		prefer=conf.get("Preference");
    	}
    	
    	public void reduce(Text key, Iterator<Text> values,
    			OutputCollector<Text, Text> output, 
                Reporter arg3) throws IOException{
    		String[] coor=key.toString().split(" ");
    		if(coor[0].equals(coor[1])){
    			output.collect(new Text(coor[0]+","+coor[1]), new Text(prefer));
    		}
    		else{
    			int i=0;
    			String[][] point=new String[2][];
    			while(values.hasNext()){
    			    point[i++]=values.next().toString().split(",");
    			}
    		    double sum=0.0;
    		    double val1,val2;
    	        for(int j=0;j<point[0].length;j++){
    	        	val1=Double.parseDouble(point[0][j]);
    	        	val2=Double.parseDouble(point[1][j]);
    	        	sum+=Math.pow(val1-val2,2);
    	        }
    	        sum=-Math.sqrt(sum);
    	        output.collect(key, new Text(String.valueOf(sum)));
    	        output.collect(new Text(coor[1]+","+coor[0]), new Text(String.valueOf(sum)));
    		}
    	}
    }
    
    public static class RMapper extends AbstractMapper{

    	protected void map(ImmutableBytesWritable rowKey,Result result,Context context)
        	    throws IOException,InterruptedException{
    		ArrayList<Double> vector=new ArrayList<Double>();
    		for(Map.Entry<byte[], byte[]> entry1:result.getFamilyMap(SIM_STRING.getBytes()).
    				entrySet()){
    			vector.add(Double.parseDouble(new String(entry1.getValue())));	
    		}
    		double max1=-Double.MAX_VALUE;
    		double max2=-Double.MAX_VALUE;
    		int column,argMax1=-1;
    		double tmp,t,value;
    		for(Map.Entry<byte[], byte[]> entry2:result.getFamilyMap(Avail_STRING1.getBytes()).
    				entrySet()){
				column=Integer.parseInt(new String(entry2.getKey()));
				value=Double.parseDouble(new String(entry2.getValue()));
    			tmp=vector.get(column-1)+value;
    			if(tmp>max1){
    				t=max1;max1=tmp;tmp=t;
    				argMax1=column;
    			}
    			if(tmp>max2){
    				max2=tmp;
    			}
    		}
    		Put putRow;
    		for(Map.Entry<byte[], byte[]> entry3:result.getFamilyMap(Res_STRING1.getBytes()).
    				entrySet()){
				column=Integer.parseInt(new String(entry3.getKey()));
				value=Double.parseDouble(new String(entry3.getValue()));
    			if(column!=argMax1){
    				value=HadoopUtil.update(value, vector.get(column-1)-max1, damping);
    			}
    			else{
    				value=HadoopUtil.update(value,vector.get(column-1)-max2, damping);
    			}
    			putRow=new Put(rowKey.get());
    			putRow.add(Res_STRING1.getBytes(),entry3.getKey(),String.valueOf(value).
    					getBytes());
    			context.write(null, putRow);
    			putRow=new Put(entry3.getKey());
    			putRow.add(Res_STRING2.getBytes(),rowKey.get(),String.valueOf(value).
    					getBytes());
    			context.write(null, putRow);
    		}
    	} 	
    }
    
    public static class AMapper extends AbstractMapper{
    	
    	protected void map(ImmutableBytesWritable rowKey,Result result,Context context)
        	    throws IOException,InterruptedException{
    		double sum=0.0;
    		double rkk=0.0;
    		int column;
    		double value;
    		Set<Map.Entry<byte[], byte[]>> resultSet=result.
    				getFamilyMap(Res_STRING2.getBytes()).entrySet();
    		ArrayList<Double> vector=new ArrayList<Double>();
    		for(int i=0;i<=result.size();i++)
    			vector.add(0.0);
    		for(Map.Entry<byte[], byte[]> entry1:resultSet){
    			column=Integer.parseInt(new String(entry1.getKey()));
    			value=Double.parseDouble(new String(entry1.getValue()));
    			if(rowKey.compareTo(entry1.getKey())!=0){
    				value=HadoopUtil.max(0.0,value);
    				vector.set(column, value);
    				sum+=value;
    			}
    			else{
    				rkk=value;
    			}
    		}
    		Put putRow;
    		for(Map.Entry<byte[], byte[]> entry2:result.getFamilyMap(Avail_STRING2.getBytes()).
    				entrySet()){
    			column=Integer.parseInt(new String(entry2.getKey()));
    			value=Double.parseDouble(new String(entry2.getValue()));
    			if(rowKey.compareTo(entry2.getKey())!=0){
    				value=HadoopUtil.update(value, HadoopUtil.min(0.0,
    						rkk+sum-vector.get(column)),damping);
    			}
    			else{
    				value=HadoopUtil.update(value, sum, damping);
    			}
        		putRow=new Put(rowKey.get());
        		putRow.add(Avail_STRING2.getBytes(), entry2.getKey(), String.valueOf(value).
        				getBytes());
        		context.write(null,putRow);
    			putRow=new Put(entry2.getKey());
    			putRow.add(Avail_STRING1.getBytes(),rowKey.get(),String.valueOf(value).
    					getBytes());
    			context.write(null, putRow);
    		}
    	}
    }
    
    public static class ExMapper extends AbstractMapper{
    	
    	protected void map(ImmutableBytesWritable rowKey,Result result,Context context)
        	    throws IOException,InterruptedException{
    		ArrayList<Double> vector=new ArrayList<Double>();
    		for(Map.Entry<byte[], byte[]> entry1:result.getFamilyMap(Avail_STRING1.getBytes()).
    				entrySet()){
    			vector.add(Double.parseDouble(new String(entry1.getValue())));
    		}
    		int column,argMax=0;
    		double value,max=-Double.MAX_VALUE;
    		for(Map.Entry<byte[], byte[]> entry2:result.getFamilyMap(Res_STRING1.getBytes()).
    				entrySet()){
    			column=Integer.parseInt(new String(entry2.getKey()));
    			value=vector.get(column-1)+Double.parseDouble(new String(entry2.getValue()));
    			if(value>max){
    				max=value;
    				argMax=column;
    			}
    		}
    		Put putRow=new Put(rowKey.get());
    		putRow.add(Exemplar_STRING.getBytes(),New_Center.getBytes(),String.valueOf(argMax).
    				getBytes());
    	    context.write(null,putRow);
    	}
    }
    
    public JobConf configureJob(Configuration conf,String jobName,String input,
        String output,Class MapClass,Class ReduceClass,
    	int mapTasks,int redTasks) throws IOException{
    	JobConf job=new JobConf(conf);
    	job.setJobName(jobName);
    	Path inputPath=new Path(input);
    	Path outputPath=new Path("output",output);
    	HadoopUtil.delete(outputPath);
    	job.setJarByClass(APCluster.class);
        job.setJar("job.jar");
    	job.setMapperClass(MapClass);
    	if(ReduceClass!=null)
    	    job.setReducerClass(ReduceClass);
    	FileInputFormat.setInputPaths(job, inputPath);
    	FileOutputFormat.setOutputPath(job, outputPath);
    	job.setNumMapTasks(mapTasks);
    	job.setNumReduceTasks(redTasks);
    	return job;
    }
    
    public int runCalSimJob(Configuration conf,String[] args)throws Exception{
		String input=args[0];
		String output="0";
		String prefer=args[5];
		int mapNum=Integer.parseInt(args[3]);
		int reduceNum=Integer.parseInt(args[4]);
		String jobName="CalSimilarity";
		conf.set("Preference", prefer);
		conf.set("record.num", args[2]);
		conf.set("key.value.separator.in.input.line", ",");
		JobConf calSimJob=configureJob(conf,jobName,input,output,SMapper.class,
				SReducer.class,mapNum,reduceNum);
		calSimJob.setInputFormat(KeyValueTextInputFormat.class);
		calSimJob.setOutputFormat(TextOutputFormat.class);
		calSimJob.setMapOutputKeyClass(Text.class);
		calSimJob.setMapOutputValueClass(Text.class);
		JobClient.runJob(calSimJob);	
		return 0;
    }
    
    public int runCalAvailJob(Configuration conf,String[] args) throws Exception{
    	String inputTable=sarTable;
    	String jobName="CalAvailability";
    	conf.set("Damping", args[3]);
    	return 0;
//    	Job calAvailJob=configureJob(conf, jobName, inputTable, inputTable, AMapper.class,
//    			null, 0, 0);
//    	return calAvailJob.waitForCompletion(true)?0:1;
    }
    
    public int runCalResJob(Configuration conf,String[] args) throws Exception{
    	String inputTable=sarTable;
    	String jobName="CalResponsibility";
    	conf.set("Damping", args[3]);
    	return 0;
//    	Job calResJob=configureJob(conf, jobName, inputTable, inputTable, RMapper.class,
//    			null, 0, 0);
//    	return calResJob.waitForCompletion(true)?0:1;
    }
    
    public boolean runCalExemplarJob(Configuration conf,String[] args,int iteration) throws Exception{
    	String inputTable=sarTable;
    	String outputTable=ExemplarTable;
    	String jobName="CalExemplar";
//    	Job calExemplarJob=configureJob(conf, jobName, inputTable, outputTable, ExMapper.class,
//    			null, 0, 0);
//    	calExemplarJob.waitForCompletion(true);
    	return isChanged(conf, outputTable, iteration);
    }
    
    public boolean isChanged(Configuration conf,String outputTable,int iteration) throws Exception{
    	HTable table=new HTable(conf, outputTable);
    	boolean changed=false;
    	long counter=0;
    	String newValue="0";
    	String value="0";
    	Put putRow;
    	for(Result row:table.getScanner(Exemplar_STRING.getBytes())){
    		for(Map.Entry<byte[], byte[]> entry:row.getFamilyMap(Exemplar_STRING.getBytes()).
    			entrySet()){
    			value=new String(entry.getValue());
    			if(counter%2==0){
    				newValue=value;
    			}
    			if(counter%2!=0&&!newValue.equals(value)){
    				changed=true;
    			}
    			if(counter%2!=0){
    				putRow=new Put(row.getRow());
        			putRow.add(Exemplar_STRING.getBytes(),Old_Center.getBytes(),newValue.getBytes());
        			table.put(putRow);
    			}
        		counter++;
    		}
    	}
    	return changed;
    }
    
    public int run(Configuration conf,String[] args) throws Exception{
    	int iteration=1;
    	int maxIteration=Integer.parseInt(args[2]);
		boolean isChanged=true;
		while(iteration<=maxIteration&&isChanged){
			runCalResJob(conf,args);
			runCalAvailJob(conf,args);
			isChanged=runCalExemplarJob(conf,args,iteration);
			iteration++;
		}
		return 0;
    }
    
    //usage: input,output,num,mapNum,reduceNum,preference,maxIteration,damping
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length<8){
			System.err.println("argument error!");
			System.exit(-1);
		}
		APCluster instance=new APCluster();
		instance.runCalSimJob(conf, otherArgs);
//		instance.run(conf,otherArgs);
	}
}
