package KMeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class RandomGenerator {
	public RandomGenerator(){}
	
	public static Path buildRandom(Path input,Path output,int numClusters,int seed) throws
	Exception{
		Path clusters=new Path(output, "0/part_seed");
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(output.toUri(),conf);
		HadoopUtil.delete(clusters);
		boolean newFile=fs.createNewFile(clusters);
		if(newFile){
			BufferedReader in=new BufferedReader(new InputStreamReader(fs.open(input)));
			ArrayList<String> list=new ArrayList<String>();
			Random random=new Random(seed);
			int currentSize=0;
			String line;
            while((line=in.readLine())!=null){
            currentSize=list.size();
            if(currentSize<numClusters){
            	list.add(line);
            }
            else{
            	if(random.nextInt(currentSize+1)==0){
            		int indexRemove=random.nextInt(currentSize);
            		list.remove(indexRemove);
            		list.add(line);
            	}
            }
            }
            char[] ch={',','0'};
            BufferedWriter out=new BufferedWriter(new OutputStreamWriter(fs.create(clusters)));
            Iterator iterator=list.iterator();
            while(iterator.hasNext()){
            	out.write((String)iterator.next());
            	out.write(ch,0,1);
            	out.write(ch,1,1);
            	out.write(ch,0,1);
            	out.write(ch,1,1);
            	out.newLine();
            }
            out.close();
            return clusters;
		}
		else{
			System.out.println("Can't create file!");
			return null;
		}	
	}
}
