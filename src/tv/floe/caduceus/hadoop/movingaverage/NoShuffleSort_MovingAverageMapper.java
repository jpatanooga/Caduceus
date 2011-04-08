package tv.floe.caduceus.hadoop.movingaverage;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class NoShuffleSort_MovingAverageMapper  extends MapReduceBase implements Mapper<LongWritable, Text, Text, TimeseriesDataPoint> 
{

	   
	static enum Timeseries_Counters { BAD_PARSE, BAD_LOOKUP };

	   
	   private JobConf configuration;
	   private final Text key = new Text();
	   private final TimeseriesDataPoint val = new TimeseriesDataPoint();
	   

	   private static final Logger logger = Logger.getLogger( NoShuffleSort_MovingAverageMapper.class );

	   
	   public void close() {
		   
		   
	   }
	   
	   public void configure(JobConf conf) {
	      this.configuration = conf;
	      
	   }
	   
	   
	   
	   
	   @Override
	   public void map(LongWritable inkey, Text value, OutputCollector<Text, TimeseriesDataPoint> output, Reporter reporter) throws IOException {

	      String line = value.toString();
	      
	      YahooStockDataPoint rec = YahooStockDataPoint.parse( line );
	      
	      if (rec != null) {
	    	  
	    		  // set both parts of the key
	    		  key.set( rec.stock_symbol );
	    	      
	    		  val.fValue = rec.getAdjustedClose();
	    	      val.lDateTime = rec.date;
	    	      
	    	      // now that its parsed, we send it through the shuffle for sort, onto reducers
	    	      output.collect(key, val);
	         
	      } else {
	    	  
	    	  reporter.incrCounter( Timeseries_Counters.BAD_PARSE, 1 );
	    	  
	      }
	    	  
	   }

}
