package TimeUrlFormat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class TimeUrlTextInputFormat extends FileInputFormat<Text, URLWritable> {
	public RecordReader<Text, URLWritable> getRecordReader(
			InputSplit input,JobConf job,Reporter reporter)
			throws IOException{
		return new TimeUrlLineRecordReader(job,(FileSplit)input);
	}

}
