package ap.v1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class APCluster {
	
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

    public static class SMapper extends AbstractMapper{
    	private String prefer;
    	private String inputTable;
    	private HTable table;
    	private HTable table2;
    	private Put putRow;
    	protected void setup(Context context) throws IOException,
	    InterruptedException{
    		Configuration conf=context.getConfiguration();
    		inputTable=conf.get("Map.Input");
    		prefer=conf.get("Preference");
    		table=new HTable(context.getConfiguration(),inputTable);
    		table2=new HTable(context.getConfiguration(), ExemplarTable);
    	}
    	
    	protected void close() throws IOException{
    		table.close();
    		table2.close();
		}
    	
    	protected void map(ImmutableBytesWritable rowKey,Result result,Context context)
        	    throws IOException,InterruptedException{
    		String insertValue1;
    		String insertValue2="0";
    		double sum,value;
    		int column;
    		ArrayList<Double> vector=new ArrayList<Double>();
    		for(Map.Entry<byte[], byte[]> entry1:result.
    				getFamilyMap(DIM_STRING.getBytes()).entrySet()){
    			vector.add(Double.parseDouble(new String(entry1.getValue())));
    		}
			putRow=new Put(rowKey.get());
			putRow.add(Exemplar_STRING.getBytes(),Old_Center.getBytes(),"0".getBytes());
			table2.put(putRow);
    		for(Result row:table.getScanner(DIM_STRING.getBytes())){
    			sum=0;
    			for(Map.Entry<byte[], byte[]> entry2:row.
    				getFamilyMap(DIM_STRING.getBytes()).entrySet()){
    				column=Integer.parseInt(new String(entry2.getKey()));
    				value=Double.parseDouble(new String(entry2.getValue()));
    				sum+=Math.pow(vector.get(column-1)-value, 2);
    			}
    			insertValue1=String.valueOf(-Math.sqrt(sum));
    			putRow=new Put(rowKey.get());
    			if(rowKey.compareTo(row.getRow())!=0){
    			    putRow.add(SIM_STRING.getBytes(),row.getRow(),insertValue1.getBytes());
    			}
    			else{
    				putRow.add(SIM_STRING.getBytes(),row.getRow(),prefer.getBytes());
    			}
    			putRow.add(Avail_STRING1.getBytes(),row.getRow(),insertValue2.getBytes());
    			putRow.add(Avail_STRING2.getBytes(),row.getRow(),insertValue2.getBytes());
    			putRow.add(Res_STRING1.getBytes(),row.getRow(),insertValue2.getBytes());    			
    			context.write(null,putRow);
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
    				value=HBaseUtil.update(value, vector.get(column-1)-max1, damping);
    			}
    			else{
    				value=HBaseUtil.update(value,vector.get(column-1)-max2, damping);
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
    				value=HBaseUtil.max(0.0,value);
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
    				value=HBaseUtil.update(value, HBaseUtil.min(0.0,
    						rkk+sum-vector.get(column)),damping);
    			}
    			else{
    				value=HBaseUtil.update(value, sum, damping);
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
    
    public Job configureJob(Configuration conf,String jobName,String inputTable,
    		String outputTable,Class MapClass,Class ReduceClass,
    		int mapTasks,int redTasks) throws IOException{
    	conf.set(TableInputFormat.INPUT_TABLE, inputTable);
    	conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable);
    	
    	Job job=new Job(conf,jobName);
    	job.setJarByClass(APCluster.class);
        ((JobConf)job.getConfiguration()).setJar("job.jar");
    	job.setMapperClass(MapClass);
    	if(ReduceClass!=null)
    	    job.setReducerClass(ReduceClass);
    	job.setNumReduceTasks(redTasks);
    	job.setInputFormatClass(TableInputFormat.class);
    	job.setOutputFormatClass(TableOutputFormat.class);
    	return job;
    }
    
    public int runCalSimJob(Configuration conf,String[] args)throws Exception{
		String inputTable=args[0];
		String prefer=args[1];
		String jobName="CalSimilarity";
		HBaseUtil.createHBaseTable(sarTable,new String[]{SIM_STRING,Avail_STRING1,
				Avail_STRING2,Res_STRING1,Res_STRING2});
		HBaseUtil.createHBaseTable(ExemplarTable, new String[]{Exemplar_STRING});
		conf.set("Map.Input", inputTable);
		conf.set("Preference", prefer);
		Job calSimJob=configureJob(conf,jobName,inputTable,sarTable,SMapper.class,
				null,0,0);
		return calSimJob.waitForCompletion(true)?0:1;	
    }
    
    public int runCalAvailJob(Configuration conf,String[] args) throws Exception{
    	String inputTable=sarTable;
    	String jobName="CalAvailability";
    	conf.set("Damping", args[3]);
    	Job calAvailJob=configureJob(conf, jobName, inputTable, inputTable, AMapper.class,
    			null, 0, 0);
    	return calAvailJob.waitForCompletion(true)?0:1;
    }
    
    public int runCalResJob(Configuration conf,String[] args) throws Exception{
    	String inputTable=sarTable;
    	String jobName="CalResponsibility";
    	conf.set("Damping", args[3]);
    	Job calResJob=configureJob(conf, jobName, inputTable, inputTable, RMapper.class,
    			null, 0, 0);
    	return calResJob.waitForCompletion(true)?0:1;
    }
    
    public boolean runCalExemplarJob(Configuration conf,String[] args,int iteration) throws Exception{
    	String inputTable=sarTable;
    	String outputTable=ExemplarTable;
    	String jobName="CalExemplar";
    	Job calExemplarJob=configureJob(conf, jobName, inputTable, outputTable, ExMapper.class,
    			null, 0, 0);
    	calExemplarJob.waitForCompletion(true);
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
    
    //usage: inputTable,preference,maxIteration,damping
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length<4){
			System.err.println("argument error!");
			System.exit(-1);
		}
		APCluster instance=new APCluster();
		instance.runCalSimJob(conf, otherArgs);
		instance.run(conf,otherArgs);
	}
}
