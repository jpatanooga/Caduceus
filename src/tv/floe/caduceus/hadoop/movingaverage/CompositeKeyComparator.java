package tv.floe.caduceus.hadoop.movingaverage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * CompositeKeyComparator
 * 
 * Purpose: Compares two WriteableComparables
 * 
 * When we are sorting keys, in this case we only want to sort by group ids in
 * that we want all of the same group ids grouped together regardless of the
 * timestamp portion of their key. This functionality is provided by the
 * NaturalKeyGroupingComparator class
 * 
 * Inside the set of k/v pairs in this group, in this secondary sort example we
 * want to sort on the second half of the key (TimeseriesKey) which is the
 * purpose of this class.
 * 
 * 
 * @author jpatterson
 * 
 */
public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(TimeseriesKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		TimeseriesKey ip1 = (TimeseriesKey) w1;
		TimeseriesKey ip2 = (TimeseriesKey) w2;

		int cmp = ip1.getGroup().compareTo(ip2.getGroup());
		if (cmp != 0) {
			return cmp;
		}

		return ip1.getTimestamp() == ip2.getTimestamp() ? 0 : (ip1
				.getTimestamp() < ip2.getTimestamp() ? -1 : 1);

	}

}