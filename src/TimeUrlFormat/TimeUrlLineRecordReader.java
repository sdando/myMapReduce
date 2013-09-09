package TimeUrlFormat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class TimeUrlLineRecordReader implements RecordReader<Text, URLWritable> {
	private KeyValueLineRecordReader lineReader;
	private Text lineKey,lineValue;
    
	public TimeUrlLineRecordReader(JobConf job,FileSplit split) throws IOException{
		lineReader=new KeyValueLineRecordReader(job, split);
		lineKey=lineReader.createKey();
		lineValue=lineReader.createValue();
	}
	
	public boolean next(Text key,URLWritable value) throws IOException{
		if(!lineReader.next(lineKey, lineValue))
			return false;
		key.set(lineKey);
		value.set(lineValue.toString());
		return true;
	}
	
	public Text createKey(){
		return new Text("");
	}
	
	public URLWritable createValue(){
		return new URLWritable();
	}
	
	public long getPos() throws IOException{
		return lineReader.getPos();
	}
	
	public float getProgress() throws IOException{
		return lineReader.getProgress();
	}
   
	public void close() throws IOException{
		lineReader.close();
	}
}
