package KMeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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


public class KMeans extends Configured implements Tool {
	
	private int maxIteration;
	
    public static class KmeansMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {
        private DmRecordParser drp ;
        private DmRecord record0;
        private DmRecord record1;

        //获取聚类中心坐标
        @Override
        public void configure(JobConf conf) {
            try {
            	Path clusters=new Path(conf.get("map.cluster"));
            	drp = new DmRecordParser(conf,clusters);
                drp.initialize(conf);
            } catch (IOException e) {
            	System.out.println("----------error-------------");
                e.printStackTrace();
            }
        }
        
        //根据聚类坐标，把文件中的点进行类别划分
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String [] collection = value.toString().split(",");
          
            ArrayList<Double> vector=new ArrayList<Double>();
            for(String element:collection){
            	vector.add(Double.parseDouble(element));
            }
            record1=new DmRecord(vector);
            
            double Min_distance = Double.MAX_VALUE;
            String tmpK=null;
            for(int i=1; i <= drp.getSize(); i++){
                record0 = drp.getCenter(String.valueOf(i));
                if(record0.squaredDistance(record1) < Min_distance){
                    tmpK = String.valueOf(i);
                    Min_distance = record0.squaredDistance(record1);
                }
            }            
            
            String record=value.toString()+","+Min_distance;
            output.collect(new Text(tmpK), new Text(record));
        }
    }
    
    //计算新的聚类中心
    public static class KmeansReducer extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {        
        private Text tValue = new Text();
        
        @Override
        public void reduce(Text key, Iterator<Text> value,
                OutputCollector<Text, Text> output, Reporter arg3)
                throws IOException {          
            double sumDistance=0;
            double tmp;
            int count=0;
            String [] strValue = null;
            ArrayList<Double> vectors=new ArrayList<Double>();
            
            while(value.hasNext()){
                count++;
                strValue = value.next().toString().split(",");
                if(count==1){
                    for(int j=0;j<strValue.length-1;j++){
                    	vectors.add(0.0);
                    }
                }
                for(int i=0;i<strValue.length-1;i++){
                	tmp=vectors.get(i)+Double.parseDouble(strValue[i]);
                	vectors.set(i, tmp);
                }
                sumDistance+=Double.parseDouble(strValue[strValue.length-1]);
            }
            String values="";
            for(int k=0;k<vectors.size();k++){
            	values+=String.valueOf(vectors.get(k)/count);
            	values+=",";
            }
            values+=String.valueOf(count);
            values+=",";
            values+=String.valueOf(sumDistance);
           
            tValue.set(values);
            output.collect(null, tValue);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception{
    	if(args.length<6){
    		System.out.println("Program needs five arguments.");
    		return -1;
    	}
    	Path input=new Path(args[0]);
    	Path output=new Path(args[1]);
    	int numClusters=Integer.parseInt(args[2]);
        maxIteration=Integer.parseInt(args[3]);
    	double delta=Double.parseDouble(args[4]);
    	int seed=Integer.parseInt(args[5]);
    	Path clusters=RandomGenerator.buildRandom(input, output, numClusters,seed);
    	return run(input,clusters,output,delta);
    }
    
    public int run(Path input,Path clusters,Path output,double delta) throws Exception{
    	int Iteration=1;
    	boolean isConverged=false;
    	Path clusterOut;
        while(Iteration<=maxIteration&&!isConverged){
            clusterOut=new Path(output,String.valueOf(Iteration));
        	isConverged=runIteration(Iteration,input,clusters,clusterOut,delta);
        	clusters=clusterOut;
        	Iteration++;
        }
        if(!isConverged){
        	System.out.println("Reach the maxIteration!");
        }
        return 0;
    }
    
    public boolean runIteration(int Iteration,Path input,Path clusters,Path output,
    		double delta) throws Exception{
    	
    	boolean isConverged=false;
    	Configuration configuration=getConf();
        JobConf conf = new JobConf(configuration, KMeans.class);

        conf.setJar("KMeans.jar");
        conf.setJobName("KMeans Cluster "+String.valueOf(Iteration));
//      conf.setNumMapTasks(8);

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
        FileInputFormat.setInputPaths(conf, input);
        FileOutputFormat.setOutputPath(conf,output);
        
        conf.set("map.cluster", clusters.toString());
        conf.setNumMapTasks(5);

        HadoopUtil.delete(output);
        JobClient.runJob(conf);
        

    	DmRecordParser rdOld=new DmRecordParser(conf, clusters);
    	DmRecordParser rdNew=new DmRecordParser(conf, output);
    	rdOld.initialize(conf);
    	rdNew.initialize(conf);
        isConverged=rdNew.equal(rdOld,delta);

    	
    	return isConverged;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new KMeans(), args);
        System.exit(exitCode);
    }
}
