import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ReadLine {
	public static void main(String[] args) throws Exception{
		Path file=new Path(args[0]);
        FileSystem fss=file.getFileSystem(new Configuration());
        BufferedReader in = new BufferedReader(new InputStreamReader(fss.open(file)));
        String line;
        while ((line = in.readLine()) != null) {
        	System.out.println(line);
        }
		
	}
}
