package KMeans_backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;


public class DmRecordParser {
    private Map<String,DmRecord> urlMap = new HashMap<String,DmRecord>();
    private Path file;
    
    public DmRecordParser(JobConf conf,Path path) throws IOException{
    	if(path==null){
    		System.out.print("path is null");
    	}
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
          while ((line = in.readLine()) != null) {
            String [] strKey = line.split("\t");
            urlMap.put(strKey[0],parse(line));
          }
        } finally {
          IOUtils.closeStream(in);
        }
      }
     
      /**
       * 生成坐标对象
       */
      public DmRecord parse(String line){
        String [] strPlate = line.split("\t");
        DmRecord Dmurl = new DmRecord(strPlate[0],
        		Double.parseDouble(strPlate[1]),Double.parseDouble(strPlate[2]),
        		Double.parseDouble(strPlate[3]));
        return Dmurl;
      }
     
      /**
       * 获取分类中心坐标
       */
      public DmRecord getUrlCode(String cluster){
          DmRecord returnCode = null;
        DmRecord dmUrl = (DmRecord)urlMap.get(cluster);
        if(dmUrl == null){
          //35     6
            returnCode = null;
        }else{
            returnCode =dmUrl;
        }
        return returnCode;
      }
      
      public double sumDistance(){
    	  Collection<DmRecord> c=urlMap.values();
    	  Iterator iterator=c.iterator();
    	  double sumDistance=0;
    	  while(iterator.hasNext()){
    		  DmRecord rd=(DmRecord)iterator.next();
    		  sumDistance+=rd.getDis();
    	  }
    	  return sumDistance;
      }
      
      public boolean equal(DmRecordParser obj){
    	  BigDecimal big1=new BigDecimal(this.sumDistance());
    	  BigDecimal big2=new BigDecimal(obj.sumDistance());
    	  return big1.compareTo(big2)==0? true : false;
    	  
      }
}

