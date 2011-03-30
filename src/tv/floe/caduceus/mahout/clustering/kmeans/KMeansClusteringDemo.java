package tv.floe.caduceus.mahout.clustering.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class KMeansClusteringDemo {
	
	public static void printClusterResults(Configuration conf, String path) throws IOException {
		
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Reader reader = new SequenceFile.Reader( fs, new Path( path ), conf );
		
		Text key = new Text();
		Text val = new Text();
		
		while ( reader.next(key, val) ) {
			
			System.out.println( key.toString() + " belongs to cluster " + val.toString() );
			
		}
		
		reader.close();
		
	}

	public static void runKMeans( String input_path, String clusters_in_path, String output_path ) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
/*
	    KMeansDriver.run(samples,
                clusters,
                output,
                measure,
                distanceThreshold,
                maxIter,
                true,
                true);		
*/		
		DistanceMeasure measure = new EuclideanDistanceMeasure();
		
		//String clusters_path = input_path + "clusters/";
		
		System.out.println( "Running KMeans with input path: " + input_path + ", clusters path: " + clusters_in_path + ", output path: " + output_path );
		
		KMeansDriver.run(new Path( input_path ), new Path( clusters_in_path ), new Path( output_path ), measure, 0.001, 10, true, false);
		
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	

}
