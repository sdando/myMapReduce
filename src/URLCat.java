import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;


public class URLCat {
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) throws Exception
	{
		InputStream in=null;
		try {
			in=new URL(args[0]).openStream();
			IOUtils.copy(in, System.out);
		}
		finally{
			IOUtils.closeQuietly(in);
		}
		
	}
}
