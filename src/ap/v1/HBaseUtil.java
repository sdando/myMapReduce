package ap.v1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class HBaseUtil {
    public static void createHBaseTable(String tableName,String[] colFam) throws IOException{
    	HTableDescriptor htd=new HTableDescriptor(tableName);
    	HColumnDescriptor col;
    	for(String column:colFam){
    		col=new HColumnDescriptor(column);
    		htd.addFamily(col);
    	}
    	Configuration configuration=HBaseConfiguration.create();
    	HBaseAdmin admin=new HBaseAdmin(configuration);
    	if(admin.tableExists(tableName)){
    		admin.disableTable(tableName);
    		admin.deleteTable(tableName);
    	}
    	admin.createTable(htd);
    }

    public static double update(double var,double newValue,double damping){
    	var=damping*var+(1-damping)*newValue;
    	return var;
    }
    
    public static double max(double a,double b){
    	return a>=b? a:b;
    }
    
    public static double min(double a,double b){
    	return a<=b? a:b;
    }
}
