package tv.floe.caduceus.mahout.clustering.kmeans;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * Should be run as hadoop jar:
 * 
 * 
 * 
 * hadoop jar IvoryMonkey_v0_1.jar tv.floe.caduceus.mahout.clustering.kmeans.Shell -init_vectors hdfs://path
 * 
 * 
 * @author jpatterson
 * 
 */
public class Shell extends Configured implements Tool {

	protected FileSystem fs;
	private Trash trash;

	/**
	   */
	public Shell() {
		this(null);
	}

	public Shell(Configuration conf) {
		super(conf);
		fs = null;
		trash = null;
	}

	protected void init() throws IOException {

		if (getConf() == null) {
			System.out.println("init > getConf() returns null!");
			return;
		}

		getConf().setQuietMode(true);
		if (this.fs == null) {
			this.fs = FileSystem.get(getConf());
		}
		if (this.trash == null) {
			this.trash = new Trash(getConf());
		}
	}

	/**
	 * Displays format of commands.
	 * 
	 */
	private static void printUsage(String cmd) {
		String prefix = "Usage: java " + Shell.class.getSimpleName();
		if ("-init_vectors".equals(cmd)) {

			System.err.println("Usage: java Shell -init_vectors <path>");
			
		} else if ("-run_kmeans".equals(cmd)) {

				System.err.println("Usage: java Shell -run_kmeans <path>");

		} else {
			System.err.println("Usage: java Shell");

			// new commands
			System.err.println("           [-init_vectors <hdfs-path>]");
			System.err.println("           [-run_kmeans <hdfs-path>]");

			System.err.println("           [-help [cmd]]");
			System.err.println();
			ToolRunner.printGenericCommandUsage(System.err);
		}
	}

	public int run(String argv[]) throws Exception {

		if (argv.length < 1) {
			printUsage("");
			return -1;
		}

		int exitCode = -1;
		int i = 0;
		String cmd = argv[i++];

		//
		// verify that we have enough command line parameters
		//
		if ("-init_vectors".equals(cmd) || "-run_kmeans".equals(cmd) ) {
			
			if (argv.length < 2) {
				printUsage(cmd);
				return exitCode;
			}
			
		} else if ( "-view_kmeans_results".equals(cmd) ) {
			
			if (argv.length < 3) {
				printUsage(cmd);
				return exitCode;
			}
					
		}

		// initialize FsShell
		try {
			init();
		} catch (RPC.VersionMismatch v) {
			System.err.println("Version Mismatch between client and server"
					+ "... command aborted.");
			return exitCode;
		} catch (IOException e) {
			System.err.println("Bad connection to FS. command aborted.");
			return exitCode;
		}

		exitCode = 0;
		try {
			if ("-init_vectors".equals(cmd)) {

				String path = argv[i++];
				
				ClusteringDemoPointFileWriter.writeClustersAndPointsToHDFS( path , this.getConf() );
				
			} else if ("-run_kmeans".equals(cmd)) {

					String in_path = argv[i++];
					
					String out_path = argv[i++];
					
					//ClusteringDemoPointFileWriter.writeClustersAndPointsToHDFS( path , this.getConf() );
					KMeansClusteringDemo.runKMeans( in_path + "points/", in_path + "clusters/", out_path );

			} else if ("-view_kmeans_results".equals(cmd)) {

				String path = argv[i++];
				
				//ClusteringDemoPointFileWriter.writeClustersAndPointsToHDFS( path , this.getConf() );
				KMeansClusteringDemo.printClusterResults(this.getConf(), path);
					
					
					
			} else {
				exitCode = -1;
				System.err.println(cmd.substring(1) + ": Unknown command");
				printUsage("");
			}
		} catch (IllegalArgumentException arge) {
			exitCode = -1;
			System.err.println(cmd.substring(1) + ": "
					+ arge.getLocalizedMessage());
			printUsage(cmd);

		} catch (Exception re) {
			exitCode = -1;
			System.err.println("Exception: " + cmd.substring(1) + ": "
					+ re.getLocalizedMessage() + ": " + re);
		} finally {
		}
		return exitCode;
	}

	public void close() throws IOException {
		if (fs != null) {
			fs.close();
			fs = null;
		}
	}

	/**
	 * main() has some simple utility methods
	 */
	public static void main(String argv[]) throws Exception {

		Shell shell = new Shell();

		int res = 0;
		try {
			res = ToolRunner.run(shell, argv);
			// shell.TestHadoopFilesystem();
		} finally {
			shell.close();
		}
		System.exit(res);
	}
}
