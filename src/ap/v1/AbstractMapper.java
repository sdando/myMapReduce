package ap.v1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class AbstractMapper extends Mapper<ImmutableBytesWritable,
Result, ImmutableBytesWritable, Put> {
    protected double damping;
	protected void setup(Context context) throws IOException,
    InterruptedException{
		Configuration conf=context.getConfiguration();
		damping=Double.parseDouble(conf.get("Damping"));
	} 
}
