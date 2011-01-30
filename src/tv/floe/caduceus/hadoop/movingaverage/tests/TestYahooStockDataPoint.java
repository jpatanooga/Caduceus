package tv.floe.caduceus.hadoop.movingaverage.tests;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import tv.floe.caduceus.hadoop.movingaverage.YahooStockDataPoint;

public class TestYahooStockDataPoint {

	@Test
	public void testParse() {
		
		String test_line = "NYSE,ZZ,2006-04-17,16.67,16.75,16.0,16.1,1440500,15.47";
				
		YahooStockDataPoint o = YahooStockDataPoint.parse(test_line);
		
		System.out.println( "symbol: " + o.stock_symbol );
		System.out.println( "close: " + o.getClose() );
		System.out.println( "date: " + o.getYearMonth() );
		
		assertEquals( "Test Symbol", "ZZ", o.stock_symbol );
		assertEquals( "Test Close", "16.1", o.close );
		assertEquals( "Test Date", "200604", o.getYearMonth() );
		assertEquals( "Test Exchange", "NYSE", o.exchange );
		assertEquals( "Test Open", "16.67", o.open );
		assertEquals( "Test Open", "16.75", o.high );
		assertEquals( "Test Open", "16.0", o.low );
		assertEquals( "Test Open", "1440500", o.volume );
		assertEquals( "Test Open", "15.47", o.adj_close );
		
	}
}
