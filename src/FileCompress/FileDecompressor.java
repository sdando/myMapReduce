package FileCompress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;


public class FileDecompressor {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		String uri=args[0];
		Configuration conf=new Configuration();
		FileSystem fsFileSystem=FileSystem.get(URI.create(uri),conf);
		
		Path inPath=new Path(uri);
		CompressionCodecFactory factory=new CompressionCodecFactory(conf);
		CompressionCodec codec=factory.getCodec(inPath);
		if(codec==null){
			System.err.println("No codec for "+uri);
			System.exit(1);
		}
		
		String outputUri=CompressionCodecFactory.removeSuffix(uri, 
				codec.getDefaultExtension());
		
		InputStream in=null;
		OutputStream out=null;
		try{
			in=codec.createInputStream(fsFileSystem.open(inPath));
			out=fsFileSystem.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		}
		finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}

}
