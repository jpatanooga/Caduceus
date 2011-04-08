package tv.floe.caduceus.hadoop.movingaverage;

import java.util.LinkedList;

/**
 * SlidingWindow
 * 
 * Very simple sliding window for timeseries processing
 * 
 * Assumes incoming points were already sorted, throws exception if value is out of order
 *
 * Window is based on the delta between the timestamps on the front and back points in the window, not the number of samples.
 * 
 * 
 * 
 */
public class SlidingWindow {

	
	LinkedList<TimeseriesDataPoint> oCurrentWindow; // = new LinkedList<Integer>();
	
	long _lWindowSize;
	long _lSlideIncrement;
	long _lCurrentTime;
	long _lSampleSize;
	
	public SlidingWindow( long WindowSizeInMS, long SlideIncrement, long sample_size ) {
	
		this._lWindowSize = WindowSizeInMS;
		this._lSlideIncrement = SlideIncrement;
		this._lCurrentTime = 0;
		this._lSampleSize = sample_size;
		
		this.oCurrentWindow = new LinkedList<TimeseriesDataPoint>();
		
	}
	
	public long GetWindowStepSize() {
		
		return this._lSlideIncrement;
		
	}
	
	public long GetWindowSize() {
		return this._lWindowSize;
	}
	
	
	public boolean WindowIsFull() {
		
		if ( this.GetWindowDelta() >= this._lWindowSize ) {
			return true;
		}
		
		return false;
		
	}
	
	public long GetWindowDelta() {
		
		if ( this.oCurrentWindow.size() > 0 ) {
			return this.oCurrentWindow.getLast().lDateTime - this.oCurrentWindow.getFirst().lDateTime + this._lSampleSize;
		}
		
		return 0;
		
	}
	
	public void AddPoint( TimeseriesDataPoint point ) throws Exception {

		// look at back of window
		
		// if back of window is greater than this point, throw exception
		if ( this.oCurrentWindow.size() > 0) {
			if ( point.lDateTime <= this.oCurrentWindow.getLast().lDateTime ) {
				throw new Exception( "Point out of order!" );
			}
		}
		
		this.oCurrentWindow.add( point );
		
	}
	
	public int GetNumberPointsInWindow() {
		
		return this.oCurrentWindow.size();
		
	}

	/**
	 * Slide the window forward
	 * - burn off the first half of the window
	 * - still must re-add more points from the Reduce iterator
	 * @throws Exception
	 */
	public void SlideWindowForward() {
		
		long lCurrentFrontTS = this.oCurrentWindow.getFirst().lDateTime; //.GetCalendar().getTimeInMillis();
		this._lCurrentTime = lCurrentFrontTS + this._lSlideIncrement;
		
		// now burn off the tail
		
		while ( this.oCurrentWindow.getFirst().lDateTime < this._lCurrentTime ) {

			this.oCurrentWindow.removeFirst();
	
		}				
		
	}	


	public LinkedList<TimeseriesDataPoint> GetCurrentWindow() {
		
		return this.oCurrentWindow;
		
	}

	
}
