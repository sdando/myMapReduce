import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {
    public static void delete(Path path) throws IOException{
    	Configuration conf=new Configuration();
    	FileSystem fs=path.getFileSystem(conf);
    	if(fs.exists(path))
    	    fs.delete(path,true);
    }
}
