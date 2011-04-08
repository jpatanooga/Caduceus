package tv.floe.caduceus.mahout.clustering.kmeans;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
//import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Based on the SimpleKMeansCluster example in Mahout in Action book
 * 
 * @author jpatterson
 *
 */
public class ClusteringDemoPointFileWriter {
	
	public static final double[][] points = { {1, 1}, {2,1}, {1,2}, {2,2}, {3,3}, {8,8}, {9,8}, {8,9}, {9,9} };
	
	public static void writePointsToSequenceFileInHDFS( List<Vector> points, String filename, FileSystem fs, Configuration conf) throws IOException {
		
		
		Path path = new Path(filename); 
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, VectorWritable.class); 
		
		long recNum = 0; 
		
		VectorWritable vec = new VectorWritable(); 
		
		for (Vector point : points) {
		
			vec.set(point); 
			
			writer.append(new LongWritable(recNum++), vec);
			
		} 
		
		writer.close();		
		
	}
	
	/**
	 * Convert array of doubles into vectors
	 * 
	 * @param raw
	 * @return
	 */
	public static List<Vector> convertPointArrayIntoList( double[][] raw ) {
		
		System.out.println( "Converting input array into a list of Vectors..." );
		
		List<Vector> points = new ArrayList<Vector>(); 
		
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
			Vector vec = null; //new RandomAccessSparseVector( 2, fr.length );// "vector: " + String.valueOf(i), fr.length);
			vec.assign(fr); 
			points.add(vec);
		} 
		
		return points;
		
		//return null;
	}
	
	/**
	 * Writes the sample points to HDFS in Sequence files to initialize the KMeans run
	 * 
	 * @param strBaseHDFSDir
	 * @param conf
	 * @throws IOException
	 */
	public static void writeClustersAndPointsToHDFS( String strBaseHDFSDir, Configuration conf ) throws IOException {
		
		System.out.println( "writeClustersAndPointsToHDFS ------- " );
		
		int k = 2;
		List<Vector> vectors = convertPointArrayIntoList(points);
		
/*		File testData = new File("testdata");
		
		if (!testData.exists()) {
			testData.mkdir();
		} 
*/	
/*
		String[] str_cmds = { "-ls", "/" };
		
		FsShell fsh = new FsShell( conf );
		try {
			fsh.run( str_cmds );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/		
		System.out.println( "Checking HDFS For: " + strBaseHDFSDir );
		
		File testData = new File( strBaseHDFSDir ); //"testdata/points");
/*		
		if (!testData.exists()) {
			//testData.mkdir();
			System.out.println( "HDFS Dir: " + strBaseHDFSDir + " does not exist, you'll need to create it." );
			return;
		}
*/		
		
		
		//Configuration conf = new Configuration(); 
		FileSystem fs = FileSystem.get(conf);
	
		System.out.println( "Writing to HDFS: " + strBaseHDFSDir + "kmeans.points" );
		
		// write the points into sequence files
		writePointsToSequenceFileInHDFS(vectors, strBaseHDFSDir + "points/kmeans.points", fs, conf);
		
		Path path = new Path( strBaseHDFSDir + "clusters/part-00000");
		
		DistanceMeasure measure = new EuclideanDistanceMeasure();
		
		System.out.println( "Writing clusters to HDFS..." );
		
		// now let's write the clusters
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Cluster.class);
		
		for (int i = 0; i < k; i++) { 
			
			Vector vec = vectors.get(i);
		
			Cluster cluster = new Cluster(vec, i, measure); 
			// cluster.addPoint(cluster.getCenter()); 
			
			writer.append(new Text(cluster.getIdentifier()), cluster);
		
		} 
		
		writer.close();			
		
		System.out.println( "Vector Init Complete." );
		
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		

		

	}

}
