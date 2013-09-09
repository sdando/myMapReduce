package KMeans_backup;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class KMeans  extends Configured implements Tool {
	
    Path clusterOut;
    Path clusterIn;
	final String output="hdfs://192.168.10.100:9000/user/zx/output/";
	private final static int iterationCount=5;

    public static class KmeansMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {
        private DmRecordParser drp ;
        private DmRecord record0 = null;
        private DmRecord record1 = new DmRecord();
        private double Min_distance = 9999.0;
        private int tmpK = 0;
        private Text tKey = new Text();
     
          
        //获取聚类中心坐标
        @Override
        public void configure(JobConf conf) {
            try {
            	Path clusterIn=new Path(conf.get("map.cluster"));
            	drp = new DmRecordParser(conf,clusterIn);
                drp.initialize(conf);
            } catch (IOException e) {
            	System.out.println("----------error-------------");
                e.printStackTrace();
            }
        }
        
        //根据聚类坐标，把文件中的点进行类别划分
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<Text, Text> output, Reporter arg3)
                throws IOException {
            String [] strArr = value.toString().split("\t");
            Min_distance = 9999.0;
            
            record1.setName(strArr[0]);
            record1.setXpoint(Double.parseDouble(strArr[1]));
            record1.setYpoint(Double.parseDouble(strArr[2]));
            
            for(int i=1; i <= 2; i++){
                record0 = drp.getUrlCode("K"+i);
                if(record0.distance(record1) < Min_distance){
                    tmpK = i;
                    Min_distance = record0.distance(record1);
                }
            }            
            
            String record=value.toString()+"\t"+Math.pow(Min_distance, 2);
            tKey.set("C"+tmpK);
            output.collect(tKey, new Text(record));
        }
    }
    
    //计算新的聚类中心
    public static class KmeansReducer extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {        
        private Text tKey = new Text();
        private Text tValue = new Text();
        
        @Override
        public void reduce(Text key, Iterator<Text> value,
                OutputCollector<Text, Text> output, Reporter arg3)
                throws IOException {
            double avgX=0;
            double avgY=0;
            double sumX=0;
            double sumY=0;
            double sum=0;
            int count=0;
            String [] strValue = null;
            
            while(value.hasNext()){
                count++;
                strValue = value.next().toString().split("\t");
                sumX = sumX + Integer.parseInt(strValue[1]);
                sumY = sumY + Integer.parseInt(strValue[2]);
                sum = sum + Double.parseDouble(strValue[3]);
            }
            
            avgX = sumX/count;
            avgY = sumY/count;
            tKey.set("K"+key.toString().substring(1,2));
            tValue.set(avgX + "\t" + avgY+"\t"+sum);
            output.collect(tKey, tValue);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception{
    	int Iteration=1;
    	boolean isConverged=false;
    	clusterIn=new Path(output+"0");
        while(Iteration<=iterationCount&&!isConverged){
            clusterOut=new Path(output,String.valueOf(Iteration));
        	isConverged=runIteration(Iteration,clusterIn,clusterOut,args);
        	clusterIn=clusterOut;
        	Iteration++;
        }
        return 0;
    }
    
    public boolean runIteration(int Iteration,Path clusterIn,Path clusterOut,
    		String[] args) throws Exception{
    	
    	boolean isConverged=false;
    	Configuration configuration=new Configuration();
        JobConf conf = new JobConf(configuration, KMeans.class);

        conf.setJobName("KMeans Cluster"+String.valueOf(Iteration));
        //conf.setNumMapTasks(200);

        // 设置Map输出的key和value的类型
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        // 设置Reduce输出的key和value的类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // 设置Mapper和Reducer
        conf.setMapperClass(KmeansMapper.class);
        conf.setReducerClass(KmeansReducer.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // 设置输入输出目录
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, clusterOut);
        
        conf.set("map.cluster", clusterIn.toString());

        delete(conf, clusterOut);
        JobClient.runJob(conf);
        
    	if(Iteration>1){
    		DmRecordParser rdOld=new DmRecordParser(conf, clusterIn);
    		DmRecordParser rdNew=new DmRecordParser(conf, clusterOut);
    		rdOld.initialize(conf);
    		rdNew.initialize(conf);
            isConverged=rdNew.equal(rdOld);
    	}
    	
    	return isConverged;
    }

    public void delete(JobConf conf,Path path) throws IOException{
    	FileSystem fs=path.getFileSystem(conf);
    	if(fs.exists(path))
    	    fs.delete(path,true);
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),new KMeans(), args);
        System.exit(exitCode);
    }
}
