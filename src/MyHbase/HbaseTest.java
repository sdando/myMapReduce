package MyHbase;


import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;



public class HbaseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		HBaseAdmin admin=new HBaseAdmin(conf);
		HTableDescriptor tableDescriptor=new HTableDescriptor("tab3".getBytes());
		tableDescriptor.addFamily(new HColumnDescriptor("fam1"));
		admin.createTable(tableDescriptor);
		
		HTable table=new HTable(conf,"tab3");
		Put putRow1=new Put("row1".getBytes());
		putRow1.add("fam1".getBytes(),"col1".getBytes(),"val1".getBytes());
		table.put(putRow1);
	    Put putRow2=new Put("row2".getBytes());
	    putRow2.add("fam1".getBytes(),"col1".getBytes(),"val2".getBytes());
	    putRow2.add("fam1".getBytes(),"col2".getBytes(),"val3".getBytes());
	    table.put(putRow2);
	    
	    for(Result row:table.getScanner("fam1".getBytes())){
	    	System.out.format("ROW\t%s\n", new String(row.getRow()));
	    	for(Map.Entry<byte[], byte[]> entry:row.getFamilyMap("fam1".getBytes()).
	    			entrySet()){
	    		String column=new String(entry.getKey());
	    		String value=new String(entry.getValue());
	    		System.out.format("Column\tfam1:%s\t%s\n", column,value);
	    	}	
	    }
	    
		

	}

}
