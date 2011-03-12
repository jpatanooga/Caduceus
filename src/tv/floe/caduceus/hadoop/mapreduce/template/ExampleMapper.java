package tv.floe.caduceus.hadoop.mapreduce.template;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * This is an example map class based off the original map reduce API.
 * 
 * @author jpatterson
 *
 */
public class ExampleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
{


	   
	   private JobConf configuration;
	   private final Text key = new Text();
	   private final Text val = new Text();
	   

	   private static final Logger logger = Logger.getLogger( ExampleMapper.class );


	   /**
	    * The close method is called ONCE after all k/v pairs for the split have been processed.
	    * 
	    */
	   public void close() {
		   
		   
	   }
	   
	   /**
	    * 
	    * The configure method is where we're going to pull in your metdata for the ETL and VAP processes
	    * from the distributed cache.
	    * 
	    * We'll plugin in the generic POJO code here.
	    * 
	    */
	   public void configure(JobConf conf) {
	      this.configuration = conf;

	      
	      // Basic pojo code here
	      
	   }
	   
	   
	   
	   /**
	    * This is the method called "per line of input" in the source file "split"
	    * 
	    * Data will come in as a k/v pair of a "LongWritable" and a "Text" value, which from a text file
	    * will be a line of text where the LongWritable represents the byte offset.
	    * 
	    * 
	    * @param inkey
	    * @param value
	    * @param output
	    * @param reporter
	    * @throws IOException
	    */
	   @Override
	   public void map(LongWritable inkey, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	      String line = value.toString();
	      boolean bExampleBadParse = false;
	      
	      /*
	       * Here we'd take the line of input text and parse it somehow, generally with POJO
	       * 
	       * Example:
	       * YahooStockDataPoint rec = YahooStockDataPoint.parse( line );
	      */
	      
	      // now if we have a valid record we'll push it on through the output collector into the Shuffle phase
	      
	      if (bExampleBadParse != false) {
	    	  
	    		  // set both parts of the key
	    		  key.set( "[key from record or derived key]" );
	    		  val.set( "Record data to push the the shuffle" );
	    	      
	    	      // now that its parsed, we send it through the shuffle for sort, onto reducers
	    	      output.collect(key, val);
	         
	      } else {
	    	  
	    	  // reporter.incrCounter( Timeseries_Counters.BAD_PARSE, 1 );
	    	  
	      }
	    	  
	   }

}
