package tv.floe.caduceus.hadoop.movingaverage.tests;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import tv.floe.caduceus.hadoop.movingaverage.SlidingWindow;
import tv.floe.caduceus.hadoop.movingaverage.TimeseriesDataPoint;
//import tv.floe.caduceus.hadoop.movingaverage.NoShuffleSort_MovingAverageReducer.PointCounters;

public class TestPreSortHeap {

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
	public void testHeapPointOrdering() {
		
   		PriorityQueue<TimeseriesDataPoint> oPointHeapNew = new PriorityQueue<TimeseriesDataPoint>();
		
    		
		TimeseriesDataPoint p_copy_0 = new TimeseriesDataPoint();
		p_copy_0.fValue = 0;
		p_copy_0.lDateTime = ParseDate( "2008-02-01" );

		TimeseriesDataPoint p_copy_1 = new TimeseriesDataPoint();
		p_copy_1.fValue = 1;
		p_copy_1.lDateTime = ParseDate( "2008-02-02" );
		
		TimeseriesDataPoint p_copy_2 = new TimeseriesDataPoint();
		p_copy_2.fValue = 2;
		p_copy_2.lDateTime = ParseDate( "2008-02-03" );
    			
		TimeseriesDataPoint p_copy_3 = new TimeseriesDataPoint();
		p_copy_3.fValue = 3;
		p_copy_3.lDateTime = ParseDate( "2009-01-01" );

		oPointHeapNew.add(p_copy_3);
		oPointHeapNew.add(p_copy_2);
    	oPointHeapNew.add(p_copy_1);
    	oPointHeapNew.add(p_copy_0);
    	
    	assertEquals( 0.0f, oPointHeapNew.peek().fValue, 0.0f );
    	
    	oPointHeapNew.poll();
		
    	assertEquals( 1.0f, oPointHeapNew.peek().fValue, 0.0f );

    	oPointHeapNew.poll();
		
    	assertEquals( 2.0f, oPointHeapNew.peek().fValue, 0.0f );
    	
    	oPointHeapNew.poll();
		
    	assertEquals( 3.0f, oPointHeapNew.peek().fValue, 0.0f );
    	
    	
    	
	}
	
	@Test
	public void testSlidingWindow() {

       	TimeseriesDataPoint next_point;
    	float point_sum = 0;
    	float moving_avg = 0;

    	// make static
    	long day_in_ms = 24 * 60 * 60 * 1000;
    	

    	// should match the width of your training samples sizes
    	int iWindowSizeInDays = 3; // this.configuration.getInt("tv.floe.examples.mr.sax.windowSize", 30 );
    	int iWindowStepSizeInDays = 1; // this.configuration.getInt("tv.floe.examples.mr.sax.windowStepSize", 1 );

    	long iWindowSizeInMS = iWindowSizeInDays * day_in_ms; // = this.configuration.getInt("tv.floe.examples.mr.sax.windowSize", 14 );
    	long iWindowStepSizeInMS = iWindowStepSizeInDays * day_in_ms; // = this.configuration.getInt("tv.floe.examples.mr.sax.windowStepSize", 7 );
    	
    	
  //  	Text out_key = new Text();
    //	Text out_val = new Text();

		SlidingWindow sliding_window = new SlidingWindow( iWindowSizeInMS, iWindowStepSizeInMS, day_in_ms );
		
		
		
		
  		PriorityQueue<TimeseriesDataPoint> oPointHeapNew = new PriorityQueue<TimeseriesDataPoint>();
		
		
		TimeseriesDataPoint p_copy_0 = new TimeseriesDataPoint();
		p_copy_0.fValue = 0;
		p_copy_0.lDateTime = ParseDate( "2008-02-01" );

		TimeseriesDataPoint p_copy_1 = new TimeseriesDataPoint();
		p_copy_1.fValue = 1;
		p_copy_1.lDateTime = ParseDate( "2008-02-02" );
		
		TimeseriesDataPoint p_copy_2 = new TimeseriesDataPoint();
		p_copy_2.fValue = 2;
		p_copy_2.lDateTime = ParseDate( "2008-02-03" );
    			
		TimeseriesDataPoint p_copy_3 = new TimeseriesDataPoint();
		p_copy_3.fValue = 3;
		p_copy_3.lDateTime = ParseDate( "2009-01-01" );

		oPointHeapNew.add(p_copy_3);
		oPointHeapNew.add(p_copy_2);
    	oPointHeapNew.add(p_copy_1);
    	oPointHeapNew.add(p_copy_0);
		
    	
    	//next_point = oPointHeapNew.poll();
		
		try {
			sliding_window.AddPoint( oPointHeapNew.poll() );
			sliding_window.AddPoint( oPointHeapNew.poll() );
			sliding_window.AddPoint( oPointHeapNew.poll() );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		assertEquals( "window full test", sliding_window.WindowIsFull(), true );
		
		if ( sliding_window.WindowIsFull() ) {
			
			//reporter.incrCounter( PointCounters.MOVING_AVERAGES_CALCD, 1 );
			
					        			
			LinkedList<TimeseriesDataPoint> oWindow = sliding_window.GetCurrentWindow();
			
			String strBackDate = oWindow.getLast().getDate();

			// ---------- compute the moving average here -----------
			
		//	out_key.set( "Group: " + key.getGroup() + ", Date: " +  strBackDate );
			
			point_sum = 0;

			for ( int x = 0; x < oWindow.size(); x++ ) {
				
				point_sum += oWindow.get(x).fValue;
				
			} // for
			
			moving_avg = point_sum / oWindow.size();
			
			assertEquals( moving_avg, 1.0f, 0.0f );
			
		//	out_val.set("Moving Average: " + moving_avg );
			
		//	output.collect( out_key, out_val );
				
			
			// 2. step window forward
			
			sliding_window.SlideWindowForward();
			
			assertEquals( "slide test", sliding_window.GetNumberPointsInWindow(), 2 );
			
		} else {
			
			//assertEquals( "window not ful" )
			
		}
		
		
	}

}
