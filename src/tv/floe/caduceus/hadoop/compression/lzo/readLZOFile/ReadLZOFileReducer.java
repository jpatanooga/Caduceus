package tv.floe.caduceus.hadoop.compression.lzo.readLZOFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class ReadLZOFileReducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> 
{
	   
	   private static final Logger logger = Logger.getLogger(ReadLZOFileReducer.class);


	   @Override
	   public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

		   Text val = new Text();
			NullWritable n = NullWritable.get();
			
			
			while (values.hasNext()) {
		
				val = values.next(); // call or be stuck in infinite loop
				
				output.collect( n, val );
				

			}
			
			
		   
	   }
	}
