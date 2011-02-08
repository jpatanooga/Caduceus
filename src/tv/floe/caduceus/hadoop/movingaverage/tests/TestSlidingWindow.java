package tv.floe.caduceus.hadoop.movingaverage.tests;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
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
	
	@Test
	public void testSimpleMovingAverage() {
		
		
       	TimeseriesDataPoint next_point;
    	float point_sum = 0;
    	float moving_avg = 0;

    	// make static
    	long day_in_ms = 24 * 60 * 60 * 1000;
    	

    	// should match the width of your training samples sizes
    	int iWindowSizeInDays = 2; //this.configuration.getInt("tv.floe.examples.mr.sax.windowSize", 30 );
    	int iWindowStepSizeInDays = 1; //this.configuration.getInt("tv.floe.examples.mr.sax.windowStepSize", 1 );

    	long iWindowSizeInMS = iWindowSizeInDays * day_in_ms; // = this.configuration.getInt("tv.floe.examples.mr.sax.windowSize", 14 );
    	long iWindowStepSizeInMS = iWindowStepSizeInDays * day_in_ms; // = this.configuration.getInt("tv.floe.examples.mr.sax.windowStepSize", 7 );
    	
    	
 //   	Text out_key = new Text();
 //   	Text out_val = new Text();

		SlidingWindow sliding_window = new SlidingWindow( iWindowSizeInMS, iWindowStepSizeInMS, day_in_ms );
		
		PriorityQueue<TimeseriesDataPoint> oPointHeapNew = new PriorityQueue<TimeseriesDataPoint>();

		TimeseriesDataPoint p_copy_0 = new TimeseriesDataPoint();
		p_copy_0.fValue = 0;
		p_copy_0.lDateTime = ParseDate( "2008-02-01" );

		oPointHeapNew.add(p_copy_0);
		
		
		TimeseriesDataPoint p_copy_1 = new TimeseriesDataPoint();
		p_copy_1.fValue = 1;
		p_copy_1.lDateTime = ParseDate( "2008-02-02" );
    		
    		oPointHeapNew.add(p_copy_1);

    		
    		TimeseriesDataPoint p_copy_2 = new TimeseriesDataPoint();
    		p_copy_2.fValue = 2;
    		p_copy_2.lDateTime = ParseDate( "2008-02-03" );
        		
        		oPointHeapNew.add(p_copy_2);
    		

		while ( oPointHeapNew.isEmpty() == false ) {
    	
			//reporter.incrCounter( PointCounters.POINTS_ADDED_TO_WINDOWS, 1 );
			
			next_point = oPointHeapNew.poll();
			
			try {
				sliding_window.AddPoint(next_point);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
    		if ( sliding_window.WindowIsFull() ) {
    			
    			//reporter.incrCounter( PointCounters.MOVING_AVERAGES_CALCD, 1 );
    			System.out.println( "calc'ing SMA --------- " );
    					        			
    			LinkedList<TimeseriesDataPoint> oWindow = sliding_window.GetCurrentWindow();
    			
    			String strBackDate = oWindow.getLast().getDate();

    			// ---------- compute the moving average here -----------
    			
    			//out_key.set( "Group: " + key.getGroup() + ", Date: " +  strBackDate );
    			
    			point_sum = 0;

    			for ( int x = 0; x < oWindow.size(); x++ ) {
    				
    				point_sum += oWindow.get(x).fValue;
    				
    			} // for
    			
    			moving_avg = point_sum / oWindow.size();
    			
    			System.out.println("Moving Average: " + moving_avg );
    			
    			//output.collect( out_key, out_val );
					
    			
    			// 2. step window forward
    			
    			sliding_window.SlideWindowForward();
    			
    		}
  		
    		
		
		}
		
		
		
	}
	
}
