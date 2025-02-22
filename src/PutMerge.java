import java.io.IOException;
import java.net.URI;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class PutMerge {
	public static void main(String[] args) throws IOException{
		Configuration conf=new Configuration();
		Path inputDir=new Path(args[0]);
		Path hdfsFile=new Path(args[1]);
		FileSystem hdfs=FileSystem.get(URI.create(args[1]),conf);  //Path必须实例化
		FileSystem local=FileSystem.get(URI.create(args[0]), conf);
		
		try {
			FileStatus[] inputFiles=local.listStatus(inputDir);
			
			FSDataOutputStream out=hdfs.create(hdfsFile);
			
			for(int i=0;i<inputFiles.length;i++)
			{
				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in=local.open(inputFiles[i].getPath());
				byte buffer[]=new byte[256];
				int bytesRead=0;
				while((bytesRead=in.read(buffer))>0){
					out.write(buffer,0,bytesRead);
				}
				in.close();
			}
			out.close();
		} catch (IOException e) {
			System.out.println("-------------error----------------");
			e.printStackTrace();
		}
	}

}
