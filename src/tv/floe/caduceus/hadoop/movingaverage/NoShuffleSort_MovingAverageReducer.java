package tv.floe.caduceus.hadoop.movingaverage;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * NoShuffleSort_MovingAverageReducer
 * 
 * In this version of the reducer the points do not arrive in pre-sorted form so
 * we have to maintain an in-memory queue to sort these points
 * 
 * 
 */

public class NoShuffleSort_MovingAverageReducer extends MapReduceBase implements
		Reducer<Text, TimeseriesDataPoint, Text, Text> {

	static enum PointCounters {
		POINTS_SEEN, POINTS_ADDED_TO_WINDOWS, MOVING_AVERAGES_CALCD
	};

	private JobConf configuration;

	@Override
	public void configure(JobConf job) {

		this.configuration = job;

	} // configure()

	public void reduce(Text key, Iterator<TimeseriesDataPoint> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		TimeseriesDataPoint next_point;
		float point_sum = 0;
		float moving_avg = 0;

		// make static
		long day_in_ms = 24 * 60 * 60 * 1000;

		// should match the width of your training samples sizes
		int iWindowSizeInDays = this.configuration.getInt(
				"tv.floe.caduceus.hadoop.movingaverage.windowSize", 30);
		int iWindowStepSizeInDays = this.configuration.getInt(
				"tv.floe.caduceus.hadoop.movingaverage.windowStepSize", 1);

		long iWindowSizeInMS = iWindowSizeInDays * day_in_ms; // =
																// this.configuration.getInt("tv.floe.examples.mr.sax.windowSize",
																// 14 );
		long iWindowStepSizeInMS = iWindowStepSizeInDays * day_in_ms; // =
																		// this.configuration.getInt("tv.floe.examples.mr.sax.windowStepSize",
																		// 7 );

		Text out_key = new Text();
		Text out_val = new Text();

		SlidingWindow sliding_window = new SlidingWindow(iWindowSizeInMS,
				iWindowStepSizeInMS, day_in_ms);

		PriorityQueue<TimeseriesDataPoint> oPointHeapNew = new PriorityQueue<TimeseriesDataPoint>();

		while (values.hasNext()) {

			next_point = values.next();

			// we need to copy the points into new objects since MR re-uses k/v
			// pairs
			// to avoid GC churn
			TimeseriesDataPoint point_copy = new TimeseriesDataPoint();
			point_copy.copy(next_point);

			oPointHeapNew.add(point_copy);

		} // while

		while (oPointHeapNew.isEmpty() == false) {

			reporter.incrCounter(PointCounters.POINTS_ADDED_TO_WINDOWS, 1);

			next_point = oPointHeapNew.poll();

			try {
				sliding_window.AddPoint(next_point);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (sliding_window.WindowIsFull()) {

				reporter.incrCounter(PointCounters.MOVING_AVERAGES_CALCD, 1);

				LinkedList<TimeseriesDataPoint> oWindow = sliding_window
						.GetCurrentWindow();

				String strBackDate = oWindow.getLast().getDate();

				// ---------- compute the moving average here -----------

				out_key.set("Group: " + key.toString() + ", Date: "
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

		}

	} // reduce

}
