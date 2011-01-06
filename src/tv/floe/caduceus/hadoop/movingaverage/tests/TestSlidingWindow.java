package tv.floe.caduceus.hadoop.movingaverage.tests;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

import tv.floe.caduceus.hadoop.movingaverage.SlidingWindow;
import tv.floe.caduceus.hadoop.movingaverage.TimeseriesDataPoint;

public class TestSlidingWindow {

	private static final String DATE_FORMAT = "yyyy-MM-dd";
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
	
	private long ParseDate( String n_Date ) {
		
		long out = 0;
		
	      try {
				out = sdf.parse( n_Date ).getTime();
				
				//System.out.println( "date: " + rec.date );
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1;
			}		
		
			return out;
	}
	
	@Test
	public void testPointOrdering() {

    	long day_in_ms = 24 * 60 * 60 * 1000;

		
		SlidingWindow window = new SlidingWindow( 2 * day_in_ms, day_in_ms, day_in_ms);
		
		TimeseriesDataPoint p_copy_0 = new TimeseriesDataPoint();
		p_copy_0.fValue = 0;
		p_copy_0.lDateTime = ParseDate( "2008-02-01" );

		TimeseriesDataPoint p_copy_1 = new TimeseriesDataPoint();
		p_copy_1.fValue = 1;
		p_copy_1.lDateTime = ParseDate( "2008-02-02" );
		
		

		try {
			window.AddPoint( p_copy_0 );
			window.AddPoint( p_copy_1 );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals( "first window test", 2, window.GetNumberPointsInWindow() );
		
		SlidingWindow window_ex = new SlidingWindow( 2 * day_in_ms, day_in_ms, day_in_ms);
		
		TimeseriesDataPoint p_copy_0_ex = new TimeseriesDataPoint();
		p_copy_0_ex.fValue = 0;
		p_copy_0_ex.lDateTime = ParseDate( "2008-02-01" );

		TimeseriesDataPoint p_copy_1_ex = new TimeseriesDataPoint();
		p_copy_1_ex.fValue = 1;
		p_copy_1_ex.lDateTime = ParseDate( "2008-02-02" );
		
		

		try {
			window_ex.AddPoint( p_copy_1_ex );
			window_ex.AddPoint( p_copy_0_ex );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println( "Exception: points out of order!" );
		}

		assertEquals( "exception window test", 1, window_ex.GetNumberPointsInWindow() );
		
		
	}
	
	@Test
	public void testWindowFull() {
	
		long day_in_ms = 24 * 60 * 60 * 1000;

		
		SlidingWindow window = new SlidingWindow( 2 * day_in_ms, day_in_ms, day_in_ms);
		
		TimeseriesDataPoint p_copy_0 = new TimeseriesDataPoint();
		p_copy_0.fValue = 0;
		p_copy_0.lDateTime = ParseDate( "2008-02-01" );

		TimeseriesDataPoint p_copy_1 = new TimeseriesDataPoint();
		p_copy_1.fValue = 1;
		p_copy_1.lDateTime = ParseDate( "2008-02-02" );
		
		

		try {
			window.AddPoint( p_copy_0 );
			window.AddPoint( p_copy_1 );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		System.out.println( "window size? " + window.GetWindowSize() );
		
		System.out.println( "window step size? " + window.GetWindowStepSize() );
		
		System.out.println( "window delta? " + window.GetWindowDelta() );
		
		System.out.println( "window full? " + window.WindowIsFull() );
		
		assertEquals( "full window", true, window.WindowIsFull() );
		
		// now check to see that the slide works right
		window.SlideWindowForward();

		
		assertEquals( "slide forward test", false, window.WindowIsFull() );
		
		
		assertEquals( "window size post slide", 86400000, window.GetWindowDelta() );
		

		TimeseriesDataPoint p_copy_2 = new TimeseriesDataPoint();
		p_copy_2.fValue = 2;
		p_copy_2.lDateTime = ParseDate( "2008-02-03" );

		TimeseriesDataPoint p_copy_3 = new TimeseriesDataPoint();
		p_copy_3.fValue = 3;
		p_copy_3.lDateTime = ParseDate( "2008-02-04" );
		
		
		try {
			window.AddPoint( p_copy_2 );
			window.AddPoint( p_copy_3 );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	

		
		assertEquals( "full window", true, window.WindowIsFull() );
		
		window.SlideWindowForward();
		
		System.out.println( "window size? " + window.GetWindowSize() );
		
		System.out.println( "window step size? " + window.GetWindowStepSize() );
		
		System.out.println( "window delta? " + window.GetWindowDelta() );
		
		System.out.println( "window full? " + window.WindowIsFull() );
		
	}
	
}
