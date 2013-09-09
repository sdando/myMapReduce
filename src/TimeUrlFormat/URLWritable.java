package TimeUrlFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.io.Writable;

public class URLWritable implements Writable{
	protected URL url;
	
	public URLWritable(){ }
	
	public URLWritable(URL url){
		this.url=url;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(url.toString());
	}
	
	public void readFields(DataInput in) throws IOException{
		url=new URL(in.readUTF());
	}
	
	public void set(String s) throws MalformedURLException {
		url=new URL(s);
	}
}
