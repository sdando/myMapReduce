import java.io.IOException; 
import java.util.Iterator; 

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.FileOutputFormat; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.mapred.KeyValueTextInputFormat; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter; 
import org.apache.hadoop.mapred.TextOutputFormat; 
import org.apache.hadoop.mapred.join.CompositeInputFormat; 
import org.apache.hadoop.mapred.join.TupleWritable; 
import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.util.ToolRunner; 

/** 
 * to run the class,  
 * 1. jar -cvf mj.jar *.class 
 * 2. rm b -rf 
 * 3. hadoop jar mj.jar OuterJoinJob a1 a2 b 
 * 4. cat b/part-00000 
 *  
 * a1: 
 * key1 val1 
 * key2 val2 
 *  
 * a2: 
 * key1 val3 
 * key2 val2 
 * key3 val3 
 *  
 * @author shepherd, email:xfzhu2008@yahoo.com 
 * 
 */ 
public class OuterJoinJob extends Configured implements Tool { 

        public static class MapClass extends MapReduceBase implements 
                        Mapper<Text, TupleWritable, Text, TupleWritable> { 

                public void map(Text key, TupleWritable value, 
                                OutputCollector<Text, TupleWritable> output, Reporter reporter) 
                                throws IOException { 
                        output.collect(key, value); 
                } 
        } 

        //Tuple相当于表的一个元组，但没有键，键放在了key中,CompositeInputFormat里有个输入，tuple的属性就有几个
        public static class ReduceClass extends MapReduceBase implements 
                        Reducer<Text, TupleWritable, Text, Text> { 
                @Override 
                public void reduce(Text key, Iterator<TupleWritable> values, 
                                OutputCollector<Text, Text> output, Reporter reporter) 
                                throws IOException { 
                        // should be just one value 
                        int count = 0; 
                        while (values.hasNext()) { 
                                System.out.println("count=======" + count); 
                                TupleWritable val = values.next(); 
                                System.out.println("val===0====" + val.get(0)); 
                                System.out.println("val===1====" + val.get(1)); 

                                if (count > 0) 
                                        throw new IOException( 
                                                        "Can't have 2 tuples for same key, there must be something wrong in the join"); 

                                //has用来判断元组的这一项是否含有
                                boolean hasOld = val.has(0); 
                                boolean hasNew = val.has(1); 

                                if (hasNew && !hasOld) { 
                                        // new record 
                                        System.out.println("new record"); 
                                        output.collect(key, (Text) val.get(1)); 
                                } else if (hasNew && hasOld) { 
                                        if (!val.get(0).equals(val.get(1))) { 
                                                // modified record 
                                                System.out.println("modified record"); 
                                                output.collect(key, (Text) val.get(1)); 
                                        } else { 
                                                System.out.println("unchanged record"); 
                                                output.collect(key, (Text) val.get(1)); 
                                        } 
                                } else if (hasOld && !hasNew) { 
                                        Text u = new Text("-" + ((Text) val.get(0)).toString()); 
                                        // remove 
                                        System.out.println("remove"); 
                                        output.collect(key, u); 
                                } 
                                count++; 
                        } 
                } 
        } 

        public int run(String[] args) throws Exception { 
                Configuration conf = getConf(); 

                JobConf job = new JobConf(conf, OuterJoinJob.class); 

                Path out = new Path(args[2]); 
                HadoopUtil.delete(out);
                FileOutputFormat.setOutputPath(job, out); 

                job.setJobName("MyJob"); 
                job.setMapperClass(MapClass.class); 
                job.setReducerClass(ReduceClass.class); 
                job.setNumReduceTasks(0);

                job.setInputFormat(CompositeInputFormat.class); 
                job.set("mapred.join.expr", CompositeInputFormat.compose("outer", 
                                KeyValueTextInputFormat.class, 
                                new String[] { args[0], args[1] })); 
                System.out.println(CompositeInputFormat.compose("outer", 
                        KeyValueTextInputFormat.class, 
                        new String[] { args[0], args[1] }));

                job.setOutputFormat(TextOutputFormat.class); 
                job.setOutputKeyClass(Text.class); 
                job.setOutputValueClass(TupleWritable.class); 

                JobClient.runJob(job); 

                return 0; 
        } 

        public static void main(String[] args) throws Exception { 
                int res = ToolRunner.run(new Configuration(), new OuterJoinJob(), args); 

                System.exit(res); 
        } 
} 