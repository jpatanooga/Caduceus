package tv.floe.caduceus.hadoop.movingaverage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * NaturalKeyGroupingComparator
 * 
 * This class is used during Hadoop's shuffle phase to group composite Key's by
 * the first part (natural) of their key.
 * 
 * 
 * 
 * @author jpatterson
 * 
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

	protected NaturalKeyGroupingComparator() {
		super(TimeseriesKey.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {

		TimeseriesKey tsK1 = (TimeseriesKey) o1;
		TimeseriesKey tsK2 = (TimeseriesKey) o2;

		return tsK1.getGroup().compareTo(tsK2.getGroup());

	}

}