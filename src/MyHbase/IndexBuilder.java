package MyHbase;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

public class IndexBuilder {

    public static final byte[] INDEX_COLUMN=Bytes.toBytes("INDEX");
    public static final byte[] INDEX_QUALIFIER=Bytes.toBytes("ROW");
    public IndexBuilder() {
		// TODO Auto-generated constructor stub
	}
    
    public static class Map extends 
        Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>{
    	private byte[] family;
    	private HashMap<byte[], ImmutableBytesWritable> indexes;
    	
    	protected void map(ImmutableBytesWritable rowKey,Result result,Context context)
    	    throws IOException,InterruptedException{
    		for(java.util.Map.Entry<byte[], ImmutableBytesWritable> index:indexes.entrySet()){
    			byte[] qualifier=index.getKey();                   //列名
//    			ImmutableBytesWritable tableName=index.getValue();
    			byte[] value=result.getValue(family, qualifier);
    			if(value!=null){
    				Put put=new Put(value);
    				put.add(INDEX_COLUMN,INDEX_QUALIFIER,rowKey.get());    				
    				context.write(null, put);
    			}
    		}
    	}
    	
    	protected void setup(Context context) throws IOException,
    	    InterruptedException{
    		Configuration conf=context.getConfiguration();
    		
    		String tableNameString=conf.get("index.tablename");
    		String[] fields=conf.getStrings("index.fields");
    		String familyName=conf.get("index.familyname");
    		family=Bytes.toBytes(familyName);
    		
    		indexes=new HashMap<byte[], ImmutableBytesWritable>();
    		for(String field:fields){
    			indexes.put(Bytes.toBytes(field), 
    					new ImmutableBytesWritable(Bytes.toBytes(tableNameString+
    							"-"+field)));
    		}
    	}
    }
    
    public static Job configureJob(Configuration conf,String args[]) throws IOException{
    	String tableName=args[0];
    	String columnFamily=args[1];
    	
//    	conf.set(TableInputFormat.SCAN, (new Scan()).toString());
    	conf.set(TableInputFormat.INPUT_TABLE, tableName);
    	
    	conf.set("index.tablename", tableName);                 //tablename为输出文件名的一部分
    	conf.set("index.familyname", columnFamily);
    	String[] fieldStrings=new String[args.length-2];
    	for(int i=0;i<fieldStrings.length;i++)
    		fieldStrings[i]=args[i+2];
    	conf.setStrings("index.fields", fieldStrings);
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName+"-"+fieldStrings[0]);
    	
    	Job job=new Job(conf,tableName);
    	job.setJarByClass(IndexBuilder.class);
    	job.setMapperClass(Map.class);
    	job.setNumReduceTasks(0);
    	job.setInputFormatClass(TableInputFormat.class);
    	job.setOutputFormatClass(TableOutputFormat.class);
    	return job;    	
    }
    
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		String[] otherArgs=new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length<3){
			System.out.println("arguments error!");
			System.exit(-1);
		}
		
		Job job=configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
