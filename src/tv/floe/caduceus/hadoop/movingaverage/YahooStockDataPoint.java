package tv.floe.caduceus.hadoop.movingaverage;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * YahooStockDataPoint
 * 
 * Primary function is to parse and hold the data from a line of Yahoo stock CSV
 * data.
 * 
 * It is the main "glue code" one would need to provide to make their timeseries
 * data source work with this example.
 * 
 * @author jpatterson
 * 
 */
public class YahooStockDataPoint {

	public String exchange;
	public String stock_symbol = "";
	public long date = 0;
	public String open = "";
	public String high = "";
	public String low = "";
	public String close = "";
	public String volume = "";
	public String adj_close = "";

	// public String segment = ""; // lookup

	private static final String DATE_FORMAT = "yyyy-MM-dd";

	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	private static final String DATE_FORMAT_YYMM = "yyyyMM";

	private static SimpleDateFormat sdf_ym = new SimpleDateFormat(
			DATE_FORMAT_YYMM);

	public String getYearMonth() {

		return sdf_ym.format(this.date);

	}

	public String getDate() {

		return sdf.format(this.date);

	}

	public float getClose() {
		// System.out.println( "close: " + this.close );
		return Float.parseFloat(this.close);

	}

	public float getAdjustedClose() {
		// System.out.println( "close: " + this.close );
		return Float.parseFloat(this.adj_close);

	}

	public static YahooStockDataPoint parse(String csvRow) {

		YahooStockDataPoint rec = new YahooStockDataPoint();

		String[] values = csvRow.split(",");

		if (values.length != 9) {
			return null;
		}

		rec.exchange = values[0].trim();
		rec.stock_symbol = values[1].trim();

		String n_Date = values[2].trim();

		try {
			rec.date = sdf.parse(n_Date).getTime();

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		rec.open = values[3].trim();
		rec.high = values[4].trim();
		rec.low = values[5].trim();
		rec.close = values[6].trim();
		rec.volume = values[7].trim();
		rec.adj_close = values[8].trim();

		return rec;

	}

}
