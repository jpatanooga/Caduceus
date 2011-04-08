package tv.floe.caduceus.hadoop.movingaverage;

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
 * MovingAverageReducer
 * 
 * Example use of secondary sort and a sliding window to produce a moving
 * average.
 * 
 * As opposed to loading all of the points into a data structure beforehand,
 * this Reducer loads only as many points as are needed to fill the window,
 * continually streaming the points through the window as it receives them.
 * 
 * 
 * Ignores the fact that values may be missing, calculates window based on time
 * delta as opposed to number of samples/points in window.
 * 
 * When only stepping one day we could get away with a simpler algorithm that
 * was more efficient, but this example is meant to show how a full sliding
 * window would work.
 * 
 * Also notice the copying of the points into the sliding window; this is
 * because Hadoop reusues Writables.
 * 
 * 
 * @author jpatterson
 * 
 */
public class MovingAverageReducer extends MapReduceBase implements
		Reducer<TimeseriesKey, TimeseriesDataPoint, Text, Text> {

	static enum PointCounters {
		POINTS_SEEN, POINTS_ADDED_TO_WINDOWS, MOVING_AVERAGES_CALCD
	};

	static long day_in_ms = 24 * 60 * 60 * 1000;

	private JobConf configuration;

	@Override
	public void configure(JobConf job) {

		this.configuration = job;

	}

	public void reduce(TimeseriesKey key, Iterator<TimeseriesDataPoint> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		TimeseriesDataPoint next_point;
		float point_sum = 0;
		float moving_avg = 0;

		int iWindowSizeInDays = this.configuration.getInt(
				"tv.floe.caduceus.hadoop.movingaverage.windowSize", 30);
		int iWindowStepSizeInDays = this.configuration.getInt(
				"tv.floe.caduceus.hadoop.movingaverage.windowStepSize", 1);

		long iWindowSizeInMS = iWindowSizeInDays * day_in_ms;
		long iWindowStepSizeInMS = iWindowStepSizeInDays * day_in_ms;

		Text out_key = new Text();
		Text out_val = new Text();

		SlidingWindow sliding_window = new SlidingWindow(iWindowSizeInMS,
				iWindowStepSizeInMS, day_in_ms);

		while (values.hasNext()) {

			while (sliding_window.WindowIsFull() == false && values.hasNext()) {

				reporter.incrCounter(PointCounters.POINTS_ADDED_TO_WINDOWS, 1);

				next_point = values.next();

				TimeseriesDataPoint p_copy = new TimeseriesDataPoint();
				p_copy.copy(next_point);

				try {
					sliding_window.AddPoint(p_copy);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			if (sliding_window.WindowIsFull()) {

				reporter.incrCounter(PointCounters.MOVING_AVERAGES_CALCD, 1);

				LinkedList<TimeseriesDataPoint> oWindow = sliding_window
						.GetCurrentWindow();

				String strBackDate = oWindow.getLast().getDate();

				// ---------- compute the moving average here -----------

				out_key.set("Group: " + key.getGroup() + ", Date: "
						+ strBackDate);

				point_sum = 0;

				for (int x = 0; x < oWindow.size(); x++) {

					point_sum += oWindow.get(x).fValue;

				} // for

				moving_avg = point_sum / oWindow.size();

				out_val.set("Moving Average: " + moving_avg);

				output.collect(out_key, out_val);

				// 2. step window forward

				sliding_window.SlideWindowForward();

			}

		} // while

		out_key.set("debug > " + key.getGroup()
				+ " --------- end of group -------------");
		out_val.set("");

		output.collect(out_key, out_val);

	} // reduce

}
