package KMeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;



public class DmRecordParser {
    private Map<String,DmRecord> urlMap = new HashMap<String,DmRecord>();
    private Path file;
    private int clusterId;
    private ArrayList<Double> clusterDistance=new ArrayList<Double>();
    private ArrayList<Integer> ptNumInCluster=new ArrayList<Integer>();
    
    public DmRecordParser(JobConf conf,Path path) throws IOException{
    	FileSystem hdfs=path.getFileSystem(conf);
    	FileStatus[] fileStatus=hdfs.listStatus(path);
    	String pathname;
    	for(FileStatus s:fileStatus)
    	{
    		pathname=s.getPath().getName();
    		if(pathname.startsWith("part")){
    			file=s.getPath();
    			break;
    		}
    	}
    }
     
      /**
       * 读取配置文件记录，生成对象
       */
      public void initialize(JobConf conf) throws IOException {
        BufferedReader in = null;
        try {
          FileSystem fss=file.getFileSystem(conf);
          in = new BufferedReader(new InputStreamReader(fss.open(file)));
          String line;
          clusterId=1;
          while ((line = in.readLine()) != null) {
            urlMap.put(String.valueOf(clusterId),parse(line));
            clusterId++;
          }
        } finally {
          IOUtils.closeStream(in);
        }
      }
     
      /**
       * 生成坐标对象
       */
      public DmRecord parse(String line){
        String [] strPlate = line.split(",");
        ArrayList<Double> arrayList=new ArrayList<Double>();
        for(int i=0;i<strPlate.length-2;i++){
        	arrayList.add(Double.parseDouble(strPlate[i]));
        }
        ptNumInCluster.add(Integer.parseInt(strPlate[strPlate.length-2]));
        clusterDistance.add(Double.parseDouble(strPlate[strPlate.length-1]));
        return new DmRecord(arrayList);
      }
     
      /**
       * 获取分类中心坐标
       */
      public DmRecord getCenter(String cluster){
        DmRecord returnRecord = null;
        DmRecord dmRecord = (DmRecord)urlMap.get(cluster);
        if(dmRecord == null){
          //35     6
        	returnRecord = null;
        }else{
        	returnRecord =dmRecord;
        }
        return returnRecord;
      }
      
      public double sumDistance(){
    	  Iterator iterator=clusterDistance.iterator();
    	  double sumDistance=0;
    	  while(iterator.hasNext()){
    		  sumDistance+=(Double)iterator.next();
    	  }
    	  return sumDistance;
      }
      
//      public boolean equal(DmRecordParser obj,double delta){
//    	  BigDecimal big1=new BigDecimal(Math.abs(this.sumDistance()-
//    			  obj.sumDistance()));
//    	  BigDecimal big2=new BigDecimal(delta);
//    	  return big1.compareTo(big2)<=0? true : false;  
//      }
//      public boolean equal(DmRecordParser obj,double delta){
//          for(int i=0;i<this.clusterDistance.size();i++){
//        	  if(Math.abs(this.clusterDistance.get(i)-
//        			  obj.clusterDistance.get(i))>delta)
//        		  return false;
//          }
//          return true; 
//      }
      
      public boolean equal(DmRecordParser obj,double delta){
          for(int i=1;i<=this.urlMap.size();i++){
        	  DmRecord record1=(DmRecord)urlMap.get(String.valueOf(i));
        	  DmRecord record2=(DmRecord)obj.urlMap.get(String.valueOf(i));
        	  if(record1.compareTo(record2, delta)==1)
        		  return false;
          }
          return true;
      }
      
      public int getSize(){
    	  return urlMap.size();
      }
}

