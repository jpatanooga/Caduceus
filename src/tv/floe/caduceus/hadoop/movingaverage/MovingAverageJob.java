package tv.floe.caduceus.hadoop.movingaverage;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * MovingAverageJob
 * 
 * This is the main job class of the Map Reduce job.
 * 
 * From here we wire together the Map, Reduce, Partition, and Writable classes.
 * 
 * @author jpatterson
 * 
 */
public class MovingAverageJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		System.out.println("\n\nMovingAverageJob\n");

		JobConf conf = new JobConf(getConf(), MovingAverageJob.class);
		conf.setJobName("MovingAverageJob");

		conf.setMapOutputKeyClass(TimeseriesKey.class);
		conf.setMapOutputValueClass(TimeseriesDataPoint.class);

		conf.setMapperClass(MovingAverageMapper.class);
		conf.setReducerClass(MovingAverageReducer.class);

		conf.setPartitionerClass(NaturalKeyPartitioner.class);
		conf.setOutputKeyComparatorClass(CompositeKeyComparator.class);
		conf.setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);

/*		
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {

					conf.setNumMapTasks(Integer.parseInt(args[++i]));

				} else if ("-r".equals(args[i])) {

					conf.setNumReduceTasks(Integer.parseInt(args[++i]));

				} else if ("-windowSize".equals(args[i])) {

					conf.set("tv.floe.caduceus.hadoop.movingaverage.windowSize", args[++i]);

				} else if ("-windowStepSize".equals(args[i])) {

					conf.set("tv.floe.caduceus.hadoop.movingaverage.windowStepSize", args[++i]);

				} else {

					other_args.add(args[i]);

				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			return printUsage();
		}
*/
		
		   List<String> other_args = new ArrayList<String>();
		   for(int i=0; i < args.length; ++i) {
		     try {
		       if ("-m".equals(args[i])) {
		       	
		         conf.setNumMapTasks(Integer.parseInt(args[++i]));
		         
		       } else if ("-r".equals(args[i])) {
		       	
		         conf.setNumReduceTasks(Integer.parseInt(args[++i]));
		    	   
		    	   
		    	   		    	   
		       } else {
		       	
		         other_args.add(args[i]);
		         
		       }
		     } catch (NumberFormatException except) {
		       System.out.println("ERROR: Integer expected instead of " + args[i]);
		       return printUsage();
		     } catch (ArrayIndexOutOfBoundsException except) {
		       System.out.println("ERROR: Required parameter missing from " +
		                          args[i-1]);
		       return printUsage();
		     }
		   }
		   // Make sure there are exactly 2 parameters left.
		   if (other_args.size() != 2) {
		     System.out.println("ERROR: Wrong number of parameters: " +
		                        other_args.size() + " instead of 2.");
		     return printUsage();
		   }
		   		
		
		conf.setInputFormat(TextInputFormat.class);

		conf.setOutputFormat(TextOutputFormat.class);
		conf.setCompressMapOutput(true);

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);

		return 0;
	}

	static int printUsage() {
		System.out
				.println("MovingAverageJob [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new MovingAverageJob(),
				args);
		System.exit(res);

	}

}
