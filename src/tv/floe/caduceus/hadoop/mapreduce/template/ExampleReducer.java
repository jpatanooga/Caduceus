package tv.floe.caduceus.hadoop.mapreduce.template;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This is the reducer task class.
 * 
 * It is meant to takes records which have been grouped by a key from the map phase and process
 * all records for the given key in some way.
 * 
 * Its function is called once per group where we loop through the collection of values for the given key.
 * 
 * @author jpatterson
 *
 */
public class ExampleReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	
    private JobConf configuration;
    
    /**
     * This method is called once before we process any kv pairs to setup any small amounts of state we might need.
     * 
     */
    @Override
    public void configure(JobConf job) {
    
    	this.configuration = job;
    	
    	
    } // configure()		
	
    /**
     * This is the actual reduce function; Unlike the map function, it is only called once per key.
     * 
     */
	 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	    	Text current_rec = null;
	    	Text out_key = new Text();
	    	Text out_val = new Text();

    		/*
    		 * Let's loop through all of the k/v pairs for this key and process them how we see fit.
    		 */
	    	while (values.hasNext()) {
	    		
	    		//iPointsSeen++;
	    		
        			
	    		current_rec = values.next();
	        		
	    			
	    			out_key.set( "[the output key]" );
	    			
	    			
	    			out_val.set( "[the outut value]" );
	    			
	    			output.collect( out_key, out_val );
						
        			
        		
        		
	    	} // while				    		
	    	
	    	
	    	
	    	
			//out_key.set( "another key?"  );
			//out_val.set( "possibly output stats here?" );
		
			output.collect( out_key, out_val );
    		
	    	
	    	
	 } // reduce

}
