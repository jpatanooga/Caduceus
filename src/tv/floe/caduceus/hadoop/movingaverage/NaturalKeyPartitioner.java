package tv.floe.caduceus.hadoop.movingaverage;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * NaturalKeyPartitioner
 * 
 * Purpose: partitions the data output from the map phase (MovingAverageMapper)
 * before it is sent through the shuffle phase.
 * 
 * The main method to pay attention to in this example is called
 * "getPartition()"
 * 
 * getPartition() determines how we group the data; In the case of this
 * secondary sort example this function partitions the data by only the first
 * half of the key, the Text group (key.getGroup().hashcode()).
 * 
 * In the case of financial data, this allows us to partition the data by stock
 * ticker name. Our key also contains the timestamp of the value, but we dont
 * want to partition by this timestamp, only the group.
 * 
 * 
 * 
 * @author jpatterson
 * 
 */
public class NaturalKeyPartitioner implements
		Partitioner<TimeseriesKey, TimeseriesDataPoint> {

	@Override
	public int getPartition(TimeseriesKey key, TimeseriesDataPoint value,
			int numPartitions) {
		return Math.abs(key.getGroup().hashCode() * 127) % numPartitions;
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}
}
