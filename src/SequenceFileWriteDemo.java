import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class SequenceFileWriteDemo {

	private static final String[] Data={
		"one ,two ,buckle my shoe",
		"three four,shut the door"
	};
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		String uri=args[0];
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		Path path=new Path(uri);
		
		IntWritable keyIntWritable=new IntWritable();
		Text valueText=new Text();
		SequenceFile.Writer writer=null;
		try{
			writer=SequenceFile.createWriter(fs, conf, path, 
					keyIntWritable.getClass(), valueText.getClass());
			for(int i=0;i<100;i++){
				keyIntWritable.set(100-i);
				valueText.set(Data[i%Data.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(),keyIntWritable,
						valueText);
				writer.append(keyIntWritable, valueText);
			}
		}
		finally{
			IOUtils.closeQuietly(writer);
		}
	}

}
