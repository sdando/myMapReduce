package MyHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class HbaseAddData {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		HTable table=new HTable(conf,"AP_TEST");
		for(int i=1;i<3;i++){
			byte[] data=String.valueOf(i).getBytes();
			Put putRow=new Put(data);
			for(int j=1;j<3;j++){
				putRow.add("DIM".getBytes(),String.valueOf(j).getBytes(),data);
			}
			table.put(putRow);
		}
	}
}
