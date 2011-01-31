package tv.floe.caduceus.hadoop.compression.lzo.readLZOFile;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class ReadLZOFileMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
{

	   
	   private JobConf configuration;

	   
	   
	   //
	   //  We reuse the same key over and over again to avoid garbage collection issues on hundreds of thousands of rows
	   //
	   //private final RecordKey key = new RecordKey();
	   private final Text key = new Text();
	   

	   private static final Logger logger = Logger.getLogger(ReadLZOFileMapper.class);

	   
	   public void configure(JobConf conf) {
	      this.configuration = conf;
	      
	      
	   }
	   
	   @Override
	   public void map(LongWritable inkey, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	      String line = value.toString();
	      
	      // figure out a keying strategy for the "group by"
	      
	      // output.collect(arg0, arg1)
	    	  
	   }

}
