package tv.floe.caduceus.hadoop.movingaverage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * MovingAverageMapper
 * 
 * The primary purpose of this map class is to read each line from the CSV file
 * split, parse out the timeseries point record with the YahooStockDataPoint
 * class, and emit a timeseries k/v pair
 * 
 * In this case we're emitting custom key and value pairs for this example in
 * the TimeseriesKey ( key / WriteableComparable ) and TimeseriesDataPoint (
 * value / Writeable )
 * 
 * @author jpatterson
 * 
 */
public class MovingAverageMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TimeseriesKey, TimeseriesDataPoint> {

	static enum Parse_Counters {
		BAD_PARSE
	};

	private JobConf configuration;
	private final TimeseriesKey key = new TimeseriesKey();
	private final TimeseriesDataPoint val = new TimeseriesDataPoint();

	private static final Logger logger = Logger
			.getLogger(MovingAverageMapper.class);

	public void close() {

	}

	public void configure(JobConf conf) {
		this.configuration = conf;

	}

	@Override
	public void map(LongWritable inkey, Text value,
			OutputCollector<TimeseriesKey, TimeseriesDataPoint> output,
			Reporter reporter) throws IOException {

		String line = value.toString();

		YahooStockDataPoint rec = YahooStockDataPoint.parse(line);

		if (rec != null) {

			// set both parts of the key
			key.set(rec.stock_symbol, rec.date);

			val.fValue = rec.getAdjustedClose();
			val.lDateTime = rec.date;

			// now that its parsed, we send it through the shuffle for sort,
			output.collect(key, val);

		} else {

			reporter.incrCounter(Parse_Counters.BAD_PARSE, 1);

		}

	}

}
